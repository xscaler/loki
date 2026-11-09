package loki

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/compactor"
	"github.com/grafana/loki/v3/pkg/storage/bucket"
	"github.com/grafana/loki/v3/pkg/validation"
)

type fakeTenantLimits map[string]*validation.Limits

func (f fakeTenantLimits) TenantLimits(id string) *validation.Limits { return f[id] }
func (f fakeTenantLimits) AllByUserID() map[string]*validation.Limits {
	return f
}

func TestAdminStoreDisabledByDefault(t *testing.T) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.PanicOnError))
	require.False(t, cfg.AdminStore.Enabled)
	require.NoError(t, cfg.AdminStore.Validate())

	orig := fakeTenantLimits{}
	l := &Loki{Cfg: cfg, TenantLimits: orig}
	svc, err := l.initAdminStore()
	require.NoError(t, err)
	require.Nil(t, svc)
	require.Equal(t, validation.TenantLimits(orig), l.TenantLimits)
}

func adminStoreLoki(t *testing.T, dir string, target ...string) *Loki {
	t.Helper()
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.PanicOnError))
	cfg.AdminStore.Enabled = true
	cfg.AdminStore.Backend = bucket.Filesystem
	cfg.AdminStore.Bucket.Filesystem.Directory = dir
	cfg.AdminStore.PollInterval = 20 * time.Millisecond
	cfg.AdminStore.StartupWait = 100 * time.Millisecond
	cfg.Target = flagext.StringSliceCSV(target)

	// initAdminStore registers on the default registerer; give each test its own.
	orig := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = orig })

	return &Loki{Cfg: cfg, TenantLimits: fakeTenantLimits{"t-runtime": {IngestionRateMB: 99}}}
}

func publishAdminStore(t *testing.T, dir string, gen int64, limits map[string]any) {
	t.Helper()
	root := filepath.Join(dir, "admin", "v1")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "limits"), 0o755))

	doc, err := json.Marshal(map[string]any{"schema": 1, "tenants": limits})
	require.NoError(t, err)
	sum := sha256.Sum256(doc)
	man, err := json.Marshal(map[string]any{
		"schema":       1,
		"generation":   gen,
		"published_at": time.Unix(1_800_000_000+gen, 0).UTC(),
		"objects":      map[string]string{"admin/v1/limits/loki.json": hex.EncodeToString(sum[:])},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "limits", "loki.json"), doc, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.json"), man, 0o644))
}

func TestAdminStoreIsOnlySourceOfTenantLimits(t *testing.T) {
	dir := t.TempDir()
	publishAdminStore(t, dir, 1, map[string]any{"t-store": map[string]any{"retention_period": "720h"}})

	l := adminStoreLoki(t, dir, Querier)
	svc, err := l.initAdminStore()
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), svc))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), svc) })

	require.NotNil(t, l.TenantLimits.TenantLimits("t-store"))
	// Runtime config tenant entries are ignored while the flag is on.
	require.Nil(t, l.TenantLimits.TenantLimits("t-runtime"))
	all := l.TenantLimits.AllByUserID()
	require.Len(t, all, 1)
	require.Contains(t, all, "t-store")
}

func TestAdminStoreCompactorWaitsForSnapshot(t *testing.T) {
	dir := t.TempDir()

	l := adminStoreLoki(t, dir, Compactor)
	l.Cfg.CompactorConfig.RetentionEnabled = true
	store, err := l.initAdminStore()
	require.NoError(t, err)
	ready, err := l.initAdminStoreReady()
	require.NoError(t, err)
	require.NotNil(t, ready)

	// The admin store itself never blocks: it runs after startup-wait.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, store))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), store) })

	// The compactor gate waits with no timeout.
	require.NoError(t, ready.StartAsync(context.Background()))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), ready) })
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, services.Starting, ready.State())

	publishAdminStore(t, dir, 1, map[string]any{"t-store": map[string]any{"retention_period": "720h"}})
	require.NoError(t, ready.AwaitRunning(context.Background()))
	require.NotNil(t, l.TenantLimits.TenantLimits("t-store"))
}

func TestAdminStoreReadyOnlyWithRetention(t *testing.T) {
	l := adminStoreLoki(t, t.TempDir(), Compactor)
	_, err := l.initAdminStore()
	require.NoError(t, err)

	l.Cfg.CompactorConfig.RetentionEnabled = false
	ready, err := l.initAdminStoreReady()
	require.NoError(t, err)
	require.Nil(t, ready)

	l.Cfg.CompactorConfig.RetentionEnabled = true
	l.Cfg.CompactorConfig.HorizontalScalingMode = compactor.HorizontalScalingModeWorker
	ready, err = l.initAdminStoreReady()
	require.NoError(t, err)
	require.Nil(t, ready)
}

// Only the Compactor module depends on the gate, so other modules in the same
// process (target=all) start without waiting for a snapshot.
func TestAdminStoreReadyGatesOnlyCompactor(t *testing.T) {
	var cfg Config
	cfg.RegisterFlags(flag.NewFlagSet("test", flag.PanicOnError))
	l := &Loki{Cfg: cfg}
	require.NoError(t, l.setupModuleManager())

	for name, deps := range l.deps {
		hasReady := false
		for _, d := range deps {
			hasReady = hasReady || d == AdminStoreReady
		}
		require.Equal(t, name == Compactor, hasReady, "module %s", name)
	}
	require.Contains(t, l.deps[Overrides], AdminStore)
	require.NotContains(t, l.deps[Overrides], AdminStoreReady)
}

func TestAdminStoreOtherTargetsContinueAfterStartupWait(t *testing.T) {
	l := adminStoreLoki(t, t.TempDir(), Querier)
	svc, err := l.initAdminStore()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, svc))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), svc) })
	require.Nil(t, l.TenantLimits.TenantLimits("t-runtime"))
}

func TestAdminStoreEnforceAuthWrapsHTTPAuthMiddleware(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	status := func(h http.Handler, path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	t.Run("off leaves the middleware alone", func(t *testing.T) {
		l := adminStoreLoki(t, t.TempDir(), Querier)
		l.Cfg.AuthEnabled = true
		l.setupAuthMiddleware()
		before := l.HTTPAuthMiddleware
		svc, err := l.initAdminStore()
		require.NoError(t, err)
		require.NotNil(t, svc)
		require.Equal(t, reflect.ValueOf(before).Pointer(), reflect.ValueOf(l.HTTPAuthMiddleware).Pointer())
	})

	t.Run("on checks authenticated routes only", func(t *testing.T) {
		l := adminStoreLoki(t, t.TempDir(), Querier)
		l.Cfg.AuthEnabled = true
		l.Cfg.AdminStore.EnforceAuth = "on"
		l.setupAuthMiddleware()
		_, err := l.initAdminStore()
		require.NoError(t, err)

		mux := http.NewServeMux()
		mux.Handle("/loki/api/v1/query", l.HTTPAuthMiddleware.Wrap(inner))
		mux.Handle("/ready", inner)
		require.Equal(t, http.StatusUnauthorized, status(mux, "/loki/api/v1/query"))
		require.Equal(t, http.StatusNoContent, status(mux, "/ready"))
	})

	t.Run("shadow never blocks", func(t *testing.T) {
		l := adminStoreLoki(t, t.TempDir(), Querier)
		l.Cfg.AuthEnabled = true
		l.Cfg.AdminStore.EnforceAuth = "shadow"
		l.setupAuthMiddleware()
		_, err := l.initAdminStore()
		require.NoError(t, err)
		// The key is unknown, yet the request reaches the handler with its own org ID.
		var org string
		h := l.HTTPAuthMiddleware.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			org, _ = user.ExtractOrgID(r.Context())
			w.WriteHeader(http.StatusNoContent)
		}))
		req := httptest.NewRequest(http.MethodGet, "/loki/api/v1/query", nil)
		req.Header.Set("Authorization", "Bearer unknown")
		req.Header.Set("X-Scope-OrgID", "tenant-a")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "tenant-a", org)
	})

	t.Run("on requires auth_enabled", func(t *testing.T) {
		l := adminStoreLoki(t, t.TempDir(), Querier)
		l.Cfg.AuthEnabled = false
		l.Cfg.AdminStore.EnforceAuth = "on"
		l.setupAuthMiddleware()
		_, err := l.initAdminStore()
		require.Error(t, err)
	})
}

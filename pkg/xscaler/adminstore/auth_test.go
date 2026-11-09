package adminstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func publishKeys(t *testing.T, b objstore.Bucket) {
	t.Helper()
	objs := map[string]string{}
	for name, body := range map[string]any{
		"tenants.json": map[string]any{"schema": 1, "tenants": map[string]any{
			"t1": map[string]any{"org_id": "t1", "state": "active", "plan_id": "p"},
		}},
		"limits/loki.json": map[string]any{"schema": 1, "tenants": map[string]any{}},
		"keys.json": map[string]any{"schema": 1, "keys": map[string]any{
			sha("rw-key"): map[string]any{"tenant_id": "t1", "scopes": []string{"read", "write"}},
			sha("ro-key"): map[string]any{"tenant_id": "t1", "scopes": []string{"read"}},
		}},
	} {
		doc, err := json.Marshal(body)
		require.NoError(t, err)
		objs[prefix+name] = sha(string(doc))
		require.NoError(t, b.Upload(context.Background(), prefix+name, bytes.NewReader(doc)))
	}
	man, err := json.Marshal(map[string]any{
		"schema": 1, "generation": 1, "published_at": time.Unix(1_800_000_001, 0).UTC(), "objects": objs,
	})
	require.NoError(t, err)
	require.NoError(t, b.Upload(context.Background(), prefix+"manifest.json", bytes.NewReader(man)))
}

func authStore(t *testing.T, mode string) *Store {
	t.Helper()
	b := objstore.NewInMemBucket()
	publishKeys(t, b)
	cfg := Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second, EnforceAuth: mode}
	s := newStoreWith(t, cfg, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })
	return s
}

type seen struct {
	called bool
	org    string
}

func serve(s *Store, prefix, method, path, token, org string) (*httptest.ResponseRecorder, *seen) {
	got := &seen{}
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got.called = true
		got.org = r.Header.Get("X-Scope-OrgID")
	})
	h := http.Handler(inner)
	if mw := s.AuthMiddleware(prefix); mw != nil {
		h = mw.Wrap(inner)
	}
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if org != "" {
		req.Header.Set("X-Scope-OrgID", org)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, got
}

func TestAuthMiddlewareOff(t *testing.T) {
	s := authStore(t, "")
	require.Nil(t, s.AuthMiddleware(""))
	s = authStore(t, "off")
	require.Nil(t, s.AuthMiddleware(""))
}

func TestAuthMiddlewareShadow(t *testing.T) {
	s := authStore(t, "shadow")
	require.NotNil(t, s.AuthMiddleware(""))

	// A denied request still reaches the handler with the org ID as sent.
	for _, c := range []struct{ token, org, path string }{
		{"", "anything", "/loki/api/v1/query"},
		{"bad", "t1", "/loki/api/v1/query"},
		{"rw-key", "other", "/loki/api/v1/query"},
		{"ro-key", "t1", "/loki/api/v1/push"},
		{"rw-key", "t1", "/ready"},
	} {
		rec, got := serve(s, "", http.MethodPost, c.path, c.token, c.org)
		require.Equal(t, http.StatusOK, rec.Code, c)
		require.True(t, got.called, c)
		require.Equal(t, c.org, got.org, c)
	}

	// An allowed request is not rewritten either.
	rec, got := serve(s, "", http.MethodGet, "/loki/api/v1/query", "rw-key", "t1")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "t1", got.org)
}

func TestAuthMiddlewareOn(t *testing.T) {
	s := authStore(t, "on")

	for _, c := range []struct {
		name, method, path, token, org string
		status                         int
		reach                          bool
	}{
		{"no token", "GET", "/loki/api/v1/query", "", "t1", 401, false},
		{"no org", "GET", "/loki/api/v1/query", "rw-key", "", 401, false},
		{"unknown token", "GET", "/loki/api/v1/query", "nope", "t1", 401, false},
		{"org mismatch", "GET", "/loki/api/v1/query", "rw-key", "t2", 401, false},
		{"anonymous org", "GET", "/loki/api/v1/query", "rw-key", "anonymous", 401, false},
		{"query", "GET", "/loki/api/v1/query", "rw-key", "t1", 200, true},
		{"query_range", "GET", "/loki/api/v1/query_range?query=x", "ro-key", "t1", 200, true},
		{"labels", "GET", "/loki/api/v1/labels", "ro-key", "t1", 200, true},
		{"series", "POST", "/loki/api/v1/series", "ro-key", "t1", 200, true},
		{"tail", "GET", "/loki/api/v1/tail", "ro-key", "t1", 200, true},
		{"patterns", "GET", "/loki/api/v1/patterns", "ro-key", "t1", 200, true},
		{"rules read", "GET", "/loki/api/v1/rules", "ro-key", "t1", 200, true},
		{"rules write", "POST", "/loki/api/v1/rules/ns", "ro-key", "t1", 403, false},
		{"rules write rw", "POST", "/loki/api/v1/rules/ns", "rw-key", "t1", 200, true},
		{"delete read", "GET", "/loki/api/v1/delete", "ro-key", "t1", 200, true},
		{"delete write", "POST", "/loki/api/v1/delete", "ro-key", "t1", 403, false},
		{"push", "POST", "/loki/api/v1/push", "rw-key", "t1", 200, true},
		{"push read only", "POST", "/loki/api/v1/push", "ro-key", "t1", 403, false},
		{"push trailing slash", "POST", "/loki/api/v1/push/", "ro-key", "t1", 403, false},
		{"push trailing slash rw", "POST", "/loki/api/v1/push/", "rw-key", "t1", 200, true},
		{"prom push", "POST", "/api/v1/push", "ro-key", "t1", 403, false},
		{"otlp", "POST", "/otlp/v1/logs", "ro-key", "t1", 403, false},
		{"otlp rw", "POST", "/otlp/v1/logs", "rw-key", "t1", 200, true},
		{"unlisted path", "GET", "/config", "rw-key", "t1", 403, false},
		{"encoded dot", "GET", "/loki/api/v1/%2e%2e/config", "rw-key", "t1", 403, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, got := serve(s, "", c.method, c.path, c.token, c.org)
			require.Equal(t, c.status, rec.Code)
			require.Equal(t, c.reach, got.called)
			if c.reach {
				require.Equal(t, "t1", got.org)
			} else {
				require.NotEmpty(t, strings.TrimSpace(rec.Body.String()))
			}
		})
	}
}

func TestAuthMiddlewareOnReplacesOrgID(t *testing.T) {
	s := authStore(t, "on")
	// Surrounding whitespace is accepted and the handler sees the exact tenant.
	rec, got := serve(s, "", http.MethodGet, "/loki/api/v1/query", "rw-key", " t1 ")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "t1", got.org)
}

func TestAuthMiddlewarePathPrefix(t *testing.T) {
	s := authStore(t, "on")
	rec, got := serve(s, "/loki-api", http.MethodPost, "/loki-api/loki/api/v1/push", "ro-key", "t1")
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.False(t, got.called)
	rec, got = serve(s, "/loki-api", http.MethodGet, "/loki-api/loki/api/v1/query", "ro-key", "t1")
	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, got.called)
}

func TestEnforceAuthValidation(t *testing.T) {
	cfg := Config{Enabled: true, PollInterval: time.Second, StartupWait: time.Second, Backend: "filesystem", EnforceAuth: "maybe"}
	require.Error(t, cfg.Validate())
	for _, v := range []string{"", "off", "shadow", "on", "ON"} {
		cfg.EnforceAuth = v
		require.NoError(t, cfg.Validate(), v)
	}
	cfg.Enabled = false
	cfg.EnforceAuth = "maybe"
	require.NoError(t, cfg.Validate())
}

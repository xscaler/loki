package adminstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
	"github.com/grafana/loki/v3/pkg/validation"
)

const prefix = "admin/v1/"

func publish(t *testing.T, b objstore.Bucket, gen int64, limits map[string]any) {
	t.Helper()
	doc, err := json.MarshalIndent(map[string]any{"schema": 1, "tenants": limits}, "", "  ")
	require.NoError(t, err)
	doc = append(doc, '\n')
	sum := sha256.Sum256(doc)

	man, err := json.Marshal(map[string]any{
		"schema":       1,
		"generation":   gen,
		"published_at": time.Unix(1_800_000_000+gen, 0).UTC(),
		"objects":      map[string]string{prefix + "limits/loki.json": hex.EncodeToString(sum[:])},
	})
	require.NoError(t, err)
	require.NoError(t, b.Upload(context.Background(), prefix+"limits/loki.json", bytes.NewReader(doc)))
	require.NoError(t, b.Upload(context.Background(), prefix+"manifest.json", bytes.NewReader(man)))
}

func newStore(t *testing.T, b objstore.Bucket) (*Store, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second}, Options{}, b, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })
	return s, reg
}

func newStoreWith(t *testing.T, cfg Config, opts Options, b objstore.Bucket, logger log.Logger, reg prometheus.Registerer) *Store {
	t.Helper()
	defaults := validation.Limits{}
	flagsetDefaults(&defaults)
	validation.SetDefaultLimitsForYAMLUnmarshalling(defaults)

	s, err := New(cfg, b, opts, logger, reg)
	require.NoError(t, err)
	return s
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	require.Eventually(t, f, 5*time.Second, 10*time.Millisecond)
}

func TestDecodeAppliesDefaults(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{
		"t-alpha": map[string]any{"retention_period": "1080h"},
		"t-gamma": map[string]any{"ingestion_rate_mb": 8, "retention_period": "336h"},
	})
	s, _ := newStore(t, b)
	tl := s.TenantLimits()

	// Startup waited for the first generation, so the values are there at once.
	alpha := tl.TenantLimits("t-alpha")
	require.NotNil(t, alpha)
	require.Equal(t, 1080*time.Hour, time.Duration(alpha.RetentionPeriod))
	gamma := tl.TenantLimits("t-gamma")
	require.Equal(t, float64(8), gamma.IngestionRateMB)
	// Fields not published keep the configured defaults.
	require.Equal(t, defaultsForTest().MaxLineSize, alpha.MaxLineSize)
	require.Nil(t, tl.TenantLimits("unknown"))
	require.Len(t, tl.AllByUserID(), 2)
}

func TestNoPublishedLimits(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{})
	s, _ := newStore(t, b)
	tl := s.TenantLimits()

	require.Nil(t, tl.TenantLimits("other"))
	require.Empty(t, tl.AllByUserID())
}

func TestDecodeFailureKeepsPrevious(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{
		"t-good": map[string]any{"retention_period": "24h"},
		"t-bad":  map[string]any{"retention_period": "48h"},
	})
	s, reg := newStore(t, b)
	tl := s.TenantLimits()
	prev := tl.TenantLimits("t-bad")
	require.NotNil(t, prev)

	publish(t, b, 2, map[string]any{
		"t-good": map[string]any{"retention_period": "36h"},
		"t-bad":  map[string]any{"no_such_limit": 1},
		"t-new":  map[string]any{"retention_period": "not-a-duration"},
	})
	eventually(t, func() bool { return tl.TenantLimits("t-good").RetentionPeriod == model36h })

	require.Same(t, prev, tl.TenantLimits("t-bad"))
	require.Nil(t, tl.TenantLimits("t-new"))

	mfs, err := reg.Gather()
	require.NoError(t, err)
	var failures float64
	for _, mf := range mfs {
		if mf.GetName() == "xscaler_admin_store_tenant_decode_failures_total" {
			for _, m := range mf.GetMetric() {
				failures += m.GetCounter().GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, failures, float64(2))
}

func TestRemovedTenantIsDropped(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{"t-a": map[string]any{"retention_period": "24h"}})
	s, _ := newStore(t, b)
	tl := s.TenantLimits()
	require.NotNil(t, tl.TenantLimits("t-a"))

	publish(t, b, 2, map[string]any{})
	eventually(t, func() bool { return tl.TenantLimits("t-a") == nil })
}

func TestStartupWaitExpires(t *testing.T) {
	b := objstore.NewInMemBucket()
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 200 * time.Millisecond}, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	start := time.Now()
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	require.GreaterOrEqual(t, time.Since(start), 200*time.Millisecond)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
}

func TestWaitAppliedBlocksUntilPublished(t *testing.T) {
	b := objstore.NewInMemBucket()
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 50 * time.Millisecond}, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	// Startup gives up after StartupWait and continues with defaults.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })

	waited := make(chan error, 1)
	go func() { waited <- s.WaitApplied(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("WaitApplied returned before anything was published: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	publish(t, b, 1, map[string]any{"t-a": map[string]any{"retention_period": "24h"}})
	select {
	case err := <-waited:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WaitApplied did not return after publish")
	}
	require.NotNil(t, s.TenantLimits().TenantLimits("t-a"))
}

func TestWaitAppliedStopsWithContext(t *testing.T) {
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond}, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, s.WaitApplied(ctx))
}

type countingBucket struct {
	objstore.Bucket
	gets atomic.Int64
}

func (c *countingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.Bucket.Get(ctx, name)
}

// A failed start must stop the reader goroutine, so the bucket stops being polled.
func TestFailedStartStopsReader(t *testing.T) {
	b := &countingBucket{Bucket: objstore.NewInMemBucket()}
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 10 * time.Millisecond, StartupWait: time.Minute}, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, s.StartAsync(ctx))
	eventually(t, func() bool { return b.gets.Load() > 0 })
	cancel()
	require.Error(t, s.AwaitRunning(context.Background()))

	// stopReader waits for the goroutine, so the count no longer moves.
	n := b.gets.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, n, b.gets.Load())
}

func TestStopWaitsForReader(t *testing.T) {
	b := &countingBucket{Bucket: objstore.NewInMemBucket()}
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 10 * time.Millisecond, StartupWait: 20 * time.Millisecond}, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))

	n := b.gets.Load()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, n, b.gets.Load())
}

func TestCacheKey(t *testing.T) {
	cfg := Config{Backend: bucket.S3}
	cfg.Bucket.S3.BucketName = "cell-a"
	require.Equal(t, "s3/cell-a//admin/v1/", cacheKey(cfg, "admin/v1/"))

	cfg = Config{Backend: bucket.GCS}
	cfg.Bucket.GCS.BucketName = "cell-b"
	require.Equal(t, "gcs/cell-b//admin/v1/", cacheKey(cfg, "admin/v1/"))

	// The storage prefix is part of the key.
	cfg = Config{Backend: bucket.S3}
	cfg.Bucket.S3.BucketName = "cell-a"
	cfg.Bucket.StoragePrefix = "tenant-x"
	require.Equal(t, "s3/cell-a/tenant-x/admin/v1/", cacheKey(cfg, "admin/v1/"))

	s := newStoreWith(t, Config{Enabled: true, Backend: bucket.S3, PollInterval: time.Second}, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	require.Equal(t, "s3///admin/v1/", cacheKey(s.cfg, "admin/v1/"))
}

// A cache written for one bucket is ignored by a store reading another.
func TestCacheIgnoredForOtherBucket(t *testing.T) {
	dir := t.TempDir()
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{"t-a": map[string]any{"retention_period": "24h"}})

	cfg := Config{Enabled: true, Backend: bucket.S3, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second, CacheDir: dir}
	cfg.Bucket.S3.BucketName = "cell-a"
	first := newStoreWith(t, cfg, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), first))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), first))

	same := newStoreWith(t, cfg, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	require.NotNil(t, same.TenantLimits().TenantLimits("t-a"))

	cfg.Bucket.S3.BucketName = "cell-b"
	other := newStoreWith(t, cfg, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	require.Nil(t, other.TenantLimits().TenantLimits("t-a"))
}

// A generation restored from the disk cache is decoded in New, so the store
// gates on it even when the bucket has nothing.
func TestCacheRestoredStateGates(t *testing.T) {
	dir := t.TempDir()
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{"t-a": map[string]any{"retention_period": "24h"}})

	cfg := Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second, CacheDir: dir}
	first := newStoreWith(t, cfg, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), first))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), first))

	// Same cache, empty bucket, and no timeout: only the cache can satisfy the gate.
	second := newStoreWith(t, cfg, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, services.StartAndAwaitRunning(ctx, second))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), second) })

	l := second.TenantLimits().TenantLimits("t-a")
	require.NotNil(t, l)
	require.Equal(t, 24*time.Hour, time.Duration(l.RetentionPeriod))
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWarnsOnRuntimeTenantsPerReload(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{})
	out := &syncBuffer{}
	reloads := make(chan interface{}, 1)
	var tenants atomic.Int32
	tenants.Store(2)

	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second},
		Options{RuntimeTenants: func() int { return int(tenants.Load()) }, RuntimeReloads: reloads},
		b, log.NewLogfmtLogger(out), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })

	count := func() int { return strings.Count(out.String(), "ignoring per-tenant limits in the runtime config") }
	require.Equal(t, 1, count())

	reloads <- struct{}{}
	eventually(t, func() bool { return count() == 2 })

	tenants.Store(0)
	reloads <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 2, count())
}

func TestCacheIgnoredForOtherStoragePrefix(t *testing.T) {
	dir := t.TempDir()
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{"t-a": map[string]any{"retention_period": "24h"}})

	cfg := Config{Enabled: true, Backend: bucket.S3, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second, CacheDir: dir}
	cfg.Bucket.S3.BucketName = "cell-a"
	cfg.Bucket.StoragePrefix = "one"
	first := newStoreWith(t, cfg, Options{}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), first))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), first))

	cfg.Bucket.StoragePrefix = "two"
	other := newStoreWith(t, cfg, Options{}, objstore.NewInMemBucket(), log.NewNopLogger(), prometheus.NewRegistry())
	require.Nil(t, other.TenantLimits().TenantLimits("t-a"))
}

func TestStopClosesRuntimeReloads(t *testing.T) {
	b := objstore.NewInMemBucket()
	publish(t, b, 1, map[string]any{})
	var closed atomic.Int32
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second},
		Options{CloseRuntimeReloads: func() { closed.Add(1) }}, b, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	require.Equal(t, int32(0), closed.Load())
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
	require.Equal(t, int32(1), closed.Load())
}

func TestFailedStartClosesRuntimeReloads(t *testing.T) {
	b := &countingBucket{Bucket: objstore.NewInMemBucket()}
	var closed atomic.Int32
	s := newStoreWith(t, Config{Enabled: true, PollInterval: 10 * time.Millisecond, StartupWait: time.Minute},
		Options{CloseRuntimeReloads: func() { closed.Add(1) }}, b, log.NewNopLogger(), prometheus.NewRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, s.StartAsync(ctx))
	eventually(t, func() bool { return b.gets.Load() > 0 })
	cancel()
	require.Error(t, s.AwaitRunning(context.Background()))
	require.Equal(t, int32(1), closed.Load())
}

func TestConfigStartupWaitMustBePositive(t *testing.T) {
	c := Config{Enabled: true, Backend: bucket.Filesystem, PollInterval: time.Second, StartupWait: time.Second}
	c.Bucket.Filesystem.Directory = t.TempDir()
	require.NoError(t, c.Validate())
	for _, d := range []time.Duration{0, -time.Second} {
		c.StartupWait = d
		require.ErrorContains(t, c.Validate(), "startup-wait must be positive")
	}
}

func TestConfigDisabledIsValid(t *testing.T) {
	c := Config{Enabled: false}
	require.NoError(t, c.Validate())
	c = Config{Enabled: true, Backend: "nope", PollInterval: time.Second}
	require.Error(t, c.Validate())
}

const model36h = model.Duration(36 * time.Hour)

func flagsetDefaults(l *validation.Limits) {
	fs := flag.NewFlagSet("test", flag.PanicOnError)
	l.RegisterFlags(fs)
	_ = fs.Parse(nil)
}

func defaultsForTest() validation.Limits {
	var l validation.Limits
	flagsetDefaults(&l)
	return l
}

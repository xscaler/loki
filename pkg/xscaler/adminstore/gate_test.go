package adminstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func publishStates(t *testing.T, b objstore.Bucket, states map[string]string) {
	t.Helper()
	tenants := map[string]any{}
	for id, st := range states {
		tenants[id] = map[string]any{"org_id": id, "state": st, "plan_id": "p"}
	}
	objs := map[string]string{}
	for name, body := range map[string]any{
		"tenants.json":     map[string]any{"schema": 1, "tenants": tenants},
		"limits/loki.json": map[string]any{"schema": 1, "tenants": map[string]any{}},
	} {
		doc, err := json.Marshal(body)
		require.NoError(t, err)
		sum := sha256.Sum256(doc)
		objs[prefix+name] = hex.EncodeToString(sum[:])
		require.NoError(t, b.Upload(context.Background(), prefix+name, bytes.NewReader(doc)))
	}
	man, err := json.Marshal(map[string]any{
		"schema": 1, "generation": 1, "published_at": time.Unix(1_800_000_001, 0).UTC(), "objects": objs,
	})
	require.NoError(t, err)
	require.NoError(t, b.Upload(context.Background(), prefix+"manifest.json", bytes.NewReader(man)))
}

func gateStore(t *testing.T, mode string, b objstore.Bucket) (*Store, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	cfg := Config{Enabled: true, PollInterval: 20 * time.Millisecond, StartupWait: 5 * time.Second, EnforceState: mode}
	s := newStoreWith(t, cfg, Options{}, b, log.NewNopLogger(), reg)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), s) })
	return s, reg
}

func TestPushGateModes(t *testing.T) {
	b := objstore.NewInMemBucket()
	publishStates(t, b, map[string]string{"locked": "soft_locked", "gone": "suspended", "ok": "active"})

	cases := []struct {
		mode     string
		wantGate bool
		deny     map[string]bool
	}{
		{"off", false, nil},
		{"shadow", true, map[string]bool{"locked": false, "gone": false, "ok": false, "nobody": false}},
		{"on", true, map[string]bool{"locked": true, "gone": false, "ok": false, "nobody": false}},
	}
	for _, c := range cases {
		t.Run(c.mode, func(t *testing.T) {
			s, reg := gateStore(t, c.mode, b)
			gate := s.PushGate()
			if !c.wantGate {
				require.Nil(t, gate)
				return
			}
			require.NotNil(t, gate)
			for tenant, want := range c.deny {
				deny, _ := gate(tenant)
				require.Equal(t, want, deny, tenant)
			}
			deny, reason := gate("locked")
			require.Equal(t, c.mode == "on", deny)
			require.Equal(t, "billing_soft_locked", reason)
			_, reason = gate("nobody")
			require.Equal(t, "unknown_tenant", reason)

			wouldDeny := testutil.ToFloat64(counter(t, reg, c.mode, "would_deny", "billing_soft_locked"))
			if c.mode == "shadow" {
				require.Positive(t, wouldDeny)
			} else {
				require.Zero(t, wouldDeny)
			}
		})
	}
}

func counter(t *testing.T, reg *prometheus.Registry, mode, decision, reason string) prometheus.Counter {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "xscaler_admin_store_state_decisions_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			if l["mode"] == mode && l["decision"] == decision && l["reason"] == reason {
				c := prometheus.NewCounter(prometheus.CounterOpts{Name: "x"})
				c.Add(m.GetCounter().GetValue())
				return c
			}
		}
	}
	return prometheus.NewCounter(prometheus.CounterOpts{Name: "x"})
}

func TestEnforceStateValidation(t *testing.T) {
	cfg := Config{Enabled: true, PollInterval: time.Second, StartupWait: time.Second, Backend: "filesystem", EnforceState: "maybe"}
	require.Error(t, cfg.Validate())
	cfg.EnforceState = "ON"
	require.NoError(t, cfg.Validate())
	cfg.Enabled = false
	cfg.EnforceState = "maybe"
	require.NoError(t, cfg.Validate())
}

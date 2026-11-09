package distributor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/loki/v3/pkg/validation"
)

func gateDistributor(t *testing.T, gate PushGate) *Distributor {
	t.Helper()
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.RejectOldSamples = false
	ds, _ := prepare(t, 1, 3, limits, nil)
	ds[0].PushGate = gate
	return ds[0]
}

func lockedGate(locked string) PushGate {
	return func(tenant string) (bool, string) {
		if tenant == locked {
			return true, "billing_soft_locked"
		}
		return false, "allowed"
	}
}

func gateRequest(t *testing.T, tenant, path, contentType string) *http.Request {
	t.Helper()
	ctx := user.InjectOrgID(context.Background(), tenant)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewReader([]byte(`{"streams":[]}`)))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	return req
}

func TestPushGateHTTP(t *testing.T) {
	handlers := map[string]struct {
		handle func(*Distributor) http.HandlerFunc
		path   string
		ctype  string
	}{
		"loki protobuf entrypoint": {func(d *Distributor) http.HandlerFunc { return d.PushHandler }, "/loki/api/v1/push", "application/x-protobuf"},
		"loki json entrypoint":     {func(d *Distributor) http.HandlerFunc { return d.PushHandler }, "/loki/api/v1/push", "application/json"},
		"otlp entrypoint":          {func(d *Distributor) http.HandlerFunc { return d.OTLPPushHandler }, "/otlp/v1/logs", "application/json"},
	}
	for name, h := range handlers {
		t.Run(name, func(t *testing.T) {
			d := gateDistributor(t, lockedGate("locked"))

			rec := httptest.NewRecorder()
			h.handle(d)(rec, gateRequest(t, "locked", h.path, h.ctype))
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
			require.Equal(t, "60", rec.Header().Get("Retry-After"))
			require.Contains(t, rec.Body.String(), "billing_soft_locked")

			// Any tenant the gate allows (unknown, suspended, active) reaches the normal path.
			rec = httptest.NewRecorder()
			h.handle(d)(rec, gateRequest(t, "other", h.path, h.ctype))
			require.NotEqual(t, http.StatusTooManyRequests, rec.Code)
			require.Empty(t, rec.Header().Get("Retry-After"))
		})
	}
}

func TestPushGateGRPC(t *testing.T) {
	d := gateDistributor(t, lockedGate("locked"))
	req := makeWriteRequest(1, 10)

	_, err := d.Push(user.InjectOrgID(context.Background(), "locked"), req)
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, err.Error(), "billing_soft_locked")

	_, err = d.Push(user.InjectOrgID(context.Background(), "other"), req)
	require.NoError(t, err)
}

func TestPushGateOffIsUpstream(t *testing.T) {
	d := gateDistributor(t, nil)

	rec := httptest.NewRecorder()
	d.PushHandler(rec, gateRequest(t, "locked", "/loki/api/v1/push", "application/json"))
	require.NotEqual(t, http.StatusTooManyRequests, rec.Code)

	_, err := d.Push(user.InjectOrgID(context.Background(), "locked"), makeWriteRequest(1, 10))
	require.NoError(t, err)
}

func TestPushGateDoesNotTouchReads(t *testing.T) {
	// The gate is only consulted by push entrypoints. ServeHTTP (the ring page)
	// is the distributor's only other HTTP surface and ignores it.
	d := gateDistributor(t, func(string) (bool, string) {
		t.Fatal("gate consulted on a read path")
		return true, ""
	})
	rec := httptest.NewRecorder()
	d.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/distributor/ring", nil))
	require.Equal(t, http.StatusOK, rec.Code)
}

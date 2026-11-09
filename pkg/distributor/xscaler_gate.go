package distributor

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/loki/v3/pkg/loghttp/push"
)

// PushGate reports whether pushes of a tenant must be denied and why.
type PushGate func(tenantID string) (deny bool, reason string)

const retryAfterSeconds = "60"

func (d *Distributor) denyReason(tenantID string) (string, bool) {
	if d.PushGate == nil {
		return "", false
	}
	deny, reason := d.PushGate(tenantID)
	return reason, deny
}

// denyPushHTTP writes a 429 with Retry-After and returns true when the tenant's
// pushes are denied.
func (d *Distributor) denyPushHTTP(w http.ResponseWriter, tenantID string, errorWriter push.ErrorWriter, logger log.Logger) bool {
	reason, deny := d.denyReason(tenantID)
	if !deny {
		return false
	}
	level.Debug(logger).Log("msg", "push denied", "reason", reason)
	w.Header().Set("Retry-After", retryAfterSeconds)
	errorWriter(w, "push denied: "+reason, http.StatusTooManyRequests, logger)
	return true
}

// pushGateGRPCError returns a ResourceExhausted error when the tenant's pushes
// are denied, nil otherwise.
func (d *Distributor) pushGateGRPCError(tenantID string) error {
	reason, deny := d.denyReason(tenantID)
	if !deny {
		return nil
	}
	return status.Error(codes.ResourceExhausted, "push denied: "+reason)
}

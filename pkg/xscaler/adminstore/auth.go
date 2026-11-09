package adminstore

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
	"github.com/xscaler/dskit/xstenant"
)

const orgIDHeader = "X-Scope-OrgID"

// AuthMiddleware returns the API key authorization middleware for the external
// HTTP API, or nil when the enforce mode is off. It must wrap the handlers
// that receive customer requests, outside the middleware that reads the org
// ID, and never routes used between components or unauthenticated routes.
//
// pathPrefix is the route prefix the HTTP server serves under, if any. In
// mode on a denied request is answered before the handler runs, which also
// covers websocket routes because the upgrade happens in the handler, and an
// allowed request gets X-Scope-OrgID set to the key's tenant. In mode shadow
// the request is never blocked or changed.
func (s *Store) AuthMiddleware(pathPrefix string) middleware.Interface {
	if s.authMode == xstenant.EnforceOff {
		return nil
	}
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d := s.reader.CheckAuth(xstenant.AuthRequest{
				Kind:          xstenant.KindLogs,
				Method:        r.Method,
				Path:          r.URL.RequestURI(),
				PathPrefix:    pathPrefix,
				Authorization: r.Header.Get("Authorization"),
				OrgID:         r.Header.Get(orgIDHeader),
			}, s.authMode)
			if !d.Allowed {
				level.Debug(s.logger).Log("msg", "request denied", "reason", d.Reason, "status", d.Status)
				http.Error(w, d.Message, d.Status)
				return
			}
			if d.Enforced {
				r.Header.Set(orgIDHeader, d.Tenant)
			}
			next.ServeHTTP(w, r)
		})
	})
}

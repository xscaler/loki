package adminstore

import (
	"flag"
	"fmt"
	"time"

	"github.com/xscaler/dskit/xstenant"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
)

const flagPrefix = "xscaler.admin-store."

// Config configures reading per-tenant limits from the xScaler admin store.
type Config struct {
	Enabled      bool          `yaml:"enabled"`
	CacheDir     string        `yaml:"cache_dir"`
	PollInterval time.Duration `yaml:"poll_interval"`
	StartupWait  time.Duration `yaml:"startup_wait"`
	EnforceState string        `yaml:"enforce_state"`
	EnforceAuth  string        `yaml:"enforce_auth"`

	Backend string        `yaml:"backend"`
	Bucket  bucket.Config `yaml:",inline"`
}

// RegisterFlags registers the flags of the admin store config.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, flagPrefix+"enabled", false, "Read per-tenant limits from the xScaler admin store in object storage. When enabled, per-tenant limits come only from the admin store and per-tenant limits in the runtime config are ignored.")
	f.StringVar(&c.CacheDir, flagPrefix+"cache-dir", "", "Directory where the last applied admin store generation is kept, so a restart can serve it while the bucket is unreachable.")
	f.DurationVar(&c.PollInterval, flagPrefix+"poll-interval", 5*time.Second, "How often the admin store manifest is polled.")
	f.DurationVar(&c.StartupWait, flagPrefix+"startup-wait", 30*time.Second, "How long to wait for the first admin store snapshot at startup before continuing with default limits. Must be positive. The compactor waits until a snapshot is applied when retention is enabled. In a process that also runs the compactor (for example -target=all or -target=backend) with retention enabled, /ready stays not ready until the first snapshot is applied while the other modules keep serving.")
	f.StringVar(&c.EnforceState, flagPrefix+"enforce-state", "off", "Deny pushes from tenants the admin store publishes as soft_locked (billing). Reads are never denied. Denied pushes get HTTP 429 with Retry-After: 60 (gRPC ResourceExhausted) and the reason billing_soft_locked. Supported values: off, shadow (count what would be denied, deny nothing), on. Only applies when the admin store is enabled.")
	f.StringVar(&c.EnforceAuth, flagPrefix+"enforce-auth", "off", "Authorize API key requests on the external HTTP API against the keys published in the admin store. A request needs an Authorization: Bearer <key> header and an X-Scope-OrgID header that names the key's tenant, and the key needs the read or write scope for the path. Requires auth_enabled. Supported values: off, shadow (count what would be denied, deny nothing, leave headers unchanged), on (deny with 401 or 403, set X-Scope-OrgID to the key's tenant). Traffic between components is never checked. Only applies when the admin store is enabled.")
	f.StringVar(&c.Backend, flagPrefix+"backend", bucket.S3, fmt.Sprintf("Backend of the admin store bucket. The bucket block below is read directly and does not depend on -use-thanos-objstore. Supported values: %v.", bucket.SupportedBackends))
	c.Bucket.RegisterFlagsWithPrefix(flagPrefix, f)
}

// Validate validates the config. A disabled config is always valid.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("%spoll-interval must be positive", flagPrefix)
	}
	if c.StartupWait <= 0 {
		return fmt.Errorf("%sstartup-wait must be positive", flagPrefix)
	}
	if _, err := c.enforceMode(); err != nil {
		return fmt.Errorf("%senforce-state: %w", flagPrefix, err)
	}
	if _, err := c.authMode(); err != nil {
		return fmt.Errorf("%senforce-auth: %w", flagPrefix, err)
	}
	supported := false
	for _, b := range bucket.SupportedBackends {
		if b == c.Backend {
			supported = true
			break
		}
	}
	if !supported {
		return fmt.Errorf("%sbackend %q is not supported", flagPrefix, c.Backend)
	}
	return c.Bucket.Validate()
}

// enforceMode parses EnforceState. An empty value means off.
func (c *Config) enforceMode() (xstenant.EnforceMode, error) {
	if c.EnforceState == "" {
		return xstenant.EnforceOff, nil
	}
	return xstenant.ParseEnforceMode(c.EnforceState)
}

// authMode parses EnforceAuth. An empty value means off.
func (c *Config) authMode() (xstenant.EnforceMode, error) {
	if c.EnforceAuth == "" {
		return xstenant.EnforceOff, nil
	}
	return xstenant.ParseEnforceMode(c.EnforceAuth)
}

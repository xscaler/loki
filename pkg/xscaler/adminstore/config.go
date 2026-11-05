package adminstore

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
)

const flagPrefix = "xscaler.admin-store."

// Config configures reading per-tenant limits from the xScaler admin store.
type Config struct {
	Enabled      bool          `yaml:"enabled"`
	CacheDir     string        `yaml:"cache_dir"`
	PollInterval time.Duration `yaml:"poll_interval"`
	StartupWait  time.Duration `yaml:"startup_wait"`

	Backend string        `yaml:"backend"`
	Bucket  bucket.Config `yaml:",inline"`
}

// RegisterFlags registers the flags of the admin store config.
func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, flagPrefix+"enabled", false, "Read per-tenant limits from the xScaler admin store in object storage. When enabled, per-tenant limits come only from the admin store and per-tenant limits in the runtime config are ignored.")
	f.StringVar(&c.CacheDir, flagPrefix+"cache-dir", "", "Directory where the last applied admin store generation is kept, so a restart can serve it while the bucket is unreachable.")
	f.DurationVar(&c.PollInterval, flagPrefix+"poll-interval", 5*time.Second, "How often the admin store manifest is polled.")
	f.DurationVar(&c.StartupWait, flagPrefix+"startup-wait", 30*time.Second, "How long to wait for the first admin store snapshot at startup before continuing with default limits. Must be positive. The compactor waits until a snapshot is applied when retention is enabled. In a process that also runs the compactor (for example -target=all or -target=backend) with retention enabled, /ready stays not ready until the first snapshot is applied while the other modules keep serving.")
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

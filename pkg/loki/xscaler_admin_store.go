package loki

import (
	"context"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/loki/v3/pkg/compactor"
	"github.com/grafana/loki/v3/pkg/storage/bucket"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/grafana/loki/v3/pkg/xscaler/adminstore"
)

// initAdminStore makes the xScaler admin store the only source of per-tenant
// limits. It does nothing unless the admin store is enabled. The runtime
// config keeps serving everything else, such as per-tenant configs.
func (t *Loki) initAdminStore() (services.Service, error) {
	if !t.Cfg.AdminStore.Enabled {
		return nil, nil
	}

	b, err := bucket.NewClient(context.Background(), t.Cfg.AdminStore.Backend, t.Cfg.AdminStore.Bucket, "xscaler-admin-store", util_log.Logger)
	if err != nil {
		return nil, err
	}

	// Decoding relies on the defaults, which initRuntimeConfig only sets when a runtime config file exists.
	validation.SetDefaultLimitsForYAMLUnmarshalling(t.Cfg.LimitsConfig)

	opts := adminstore.Options{}
	if t.runtimeConfig != nil {
		opts.RuntimeTenants = func() int {
			if cfg, ok := t.runtimeConfig.GetConfig().(*runtimeConfigValues); ok && cfg != nil {
				return len(cfg.TenantLimits)
			}
			return 0
		}
		reloads := t.runtimeConfig.CreateListenerChannel(1)
		opts.RuntimeReloads = reloads
		opts.CloseRuntimeReloads = func() { t.runtimeConfig.CloseListenerChannel(reloads) }
	}

	store, err := adminstore.New(t.Cfg.AdminStore, b, opts, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}
	t.adminStore = store
	t.TenantLimits = store.TenantLimits()
	return store, nil
}

// initAdminStoreReady holds back the compactor until the admin store has
// applied a snapshot, so retention never runs on default limits. It does
// nothing unless the admin store and compactor retention are both enabled.
// Only modules that depend on it wait, so ingest and query targets running
// in the same process start as soon as the admin store is up.
func (t *Loki) initAdminStoreReady() (services.Service, error) {
	if t.adminStore == nil || !t.Cfg.CompactorConfig.RetentionEnabled ||
		t.Cfg.CompactorConfig.HorizontalScalingMode == compactor.HorizontalScalingModeWorker {
		return nil, nil
	}
	starting := func(ctx context.Context) error {
		level.Info(util_log.Logger).Log("msg", "waiting for the first admin store snapshot before starting the compactor")
		return t.adminStore.WaitApplied(ctx)
	}
	running := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}
	return services.NewBasicService(starting, running, nil), nil
}

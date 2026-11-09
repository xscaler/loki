// Package adminstore applies per-tenant limits published to the xScaler admin
// store. It only decodes and serves published values.
package adminstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/xscaler/dskit/xstenant"
	"gopkg.in/yaml.v2"

	"github.com/grafana/loki/v3/pkg/storage/bucket"
	"github.com/grafana/loki/v3/pkg/validation"
)

const engine = "loki"

// Options are the parts of Store that depend on how Loki is wired.
type Options struct {
	// RuntimeTenants returns the number of per-tenant limit entries in the
	// runtime config. Those entries are ignored, so a warning is logged when
	// there are any. May be nil.
	RuntimeTenants func() int
	// RuntimeReloads delivers one value per runtime config reload. May be nil.
	RuntimeReloads <-chan interface{}
	// CloseRuntimeReloads unregisters RuntimeReloads from the runtime config.
	// It is called once when the store stops. May be nil.
	CloseRuntimeReloads func()
}

// Store serves the per-tenant limits of the latest admin store generation.
type Store struct {
	services.Service

	cfg         Config
	reader      *xstenant.Reader
	enforceMode xstenant.EnforceMode
	authMode    xstenant.EnforceMode
	source      *xstenant.Source[*validation.Limits]
	startupWait time.Duration
	opts        Options
	logger      log.Logger
	stopReader  func()
	closeOnce   sync.Once
}

// New returns a Store reading from bucket. validation.SetDefaultLimitsForYAMLUnmarshalling
// must have been called before New, which applies a snapshot restored from the disk cache.
func New(cfg Config, bucket xstenant.Bucket, opts Options, logger log.Logger, reg prometheus.Registerer) (*Store, error) {
	rcfg := xstenant.DefaultConfig()
	rcfg.CacheDir = cfg.CacheDir
	rcfg.PollInterval = cfg.PollInterval
	rcfg.CacheKey = cacheKey(cfg, rcfg.Prefix)

	mode, err := cfg.enforceMode()
	if err != nil {
		return nil, fmt.Errorf("%senforce-state: %w", flagPrefix, err)
	}

	authMode, err := cfg.authMode()
	if err != nil {
		return nil, fmt.Errorf("%senforce-auth: %w", flagPrefix, err)
	}

	logger = log.With(logger, "component", "xscaler-admin-store")
	reader, err := xstenant.New(rcfg, bucket, logger, reg)
	if err != nil {
		return nil, err
	}

	s := &Store{
		cfg:         cfg,
		reader:      reader,
		enforceMode: mode,
		authMode:    authMode,
		source:      xstenant.NewSource(reader, engine, decode, logger, reg),
		startupWait: cfg.StartupWait,
		opts:        opts,
		logger:      logger,
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *Store) starting(ctx context.Context) error {
	s.stopReader = s.reader.Start(ctx)

	if s.startupWait > 0 {
		if err := s.source.WaitApplied(ctx, s.startupWait); err != nil {
			if ctx.Err() != nil {
				s.stopReader()
				s.closeReloads()
				return ctx.Err()
			}
			level.Warn(s.logger).Log("msg", "admin store not applied, continuing with default limits", "wait", s.startupWait, "err", err)
		}
	}
	s.warnRuntimeTenants()
	return nil
}

func (s *Store) stopping(_ error) error {
	s.stopReader()
	s.closeReloads()
	return nil
}

func (s *Store) closeReloads() {
	s.closeOnce.Do(func() {
		if s.opts.CloseRuntimeReloads != nil {
			s.opts.CloseRuntimeReloads()
		}
	})
}

// WaitApplied blocks until a snapshot with Loki limits is applied or ctx is
// done. Callers that must not run on default limits, such as the compactor
// with retention, use it before they start.
func (s *Store) WaitApplied(ctx context.Context) error {
	return s.source.WaitApplied(ctx, 0)
}

// cacheKey identifies the admin bucket and prefix, so a disk cache written
// for another bucket is ignored.
func cacheKey(cfg Config, prefix string) string {
	var name string
	b := cfg.Bucket
	switch cfg.Backend {
	case bucket.S3:
		name = b.S3.BucketName
	case bucket.GCS:
		name = b.GCS.BucketName
	case bucket.Azure:
		name = b.Azure.ContainerName
	case bucket.Swift:
		name = b.Swift.ContainerName
	case bucket.Filesystem:
		name = b.Filesystem.Directory
	case bucket.Alibaba:
		name = b.Alibaba.Bucket
	case bucket.BOS:
		name = b.BOS.Bucket
	}
	return cfg.Backend + "/" + name + "/" + cfg.Bucket.StoragePrefix + "/" + prefix
}

func (s *Store) running(ctx context.Context) error {
	reloads := s.opts.RuntimeReloads
	for {
		select {
		case <-ctx.Done():
			return nil
		case _, ok := <-reloads:
			if !ok {
				reloads = nil
				continue
			}
			s.warnRuntimeTenants()
		}
	}
}

// warnRuntimeTenants logs once if the runtime config carries per-tenant
// limits, which are ignored while the admin store is enabled.
func (s *Store) warnRuntimeTenants() {
	if s.opts.RuntimeTenants == nil {
		return
	}
	if n := s.opts.RuntimeTenants(); n > 0 {
		level.Warn(s.logger).Log("msg", "ignoring per-tenant limits in the runtime config, the admin store is the only source", "tenants", n)
	}
}

// decode parses one tenant block. JSON is valid YAML, so the usual limits
// unmarshalling applies, including defaults.
func decode(_ string, doc json.RawMessage) (*validation.Limits, error) {
	l := &validation.Limits{}
	if err := yaml.UnmarshalStrict(doc, l); err != nil {
		return nil, err
	}
	if err := l.Validate(); err != nil {
		return nil, fmt.Errorf("invalid limits: %w", err)
	}
	return l, nil
}

// TenantLimits returns a validation.TenantLimits backed only by the admin
// store. A tenant without an entry gets nil, which means the default limits.
func (s *Store) TenantLimits() validation.TenantLimits {
	return tenantLimits{source: s.source}
}

type tenantLimits struct {
	source *xstenant.Source[*validation.Limits]
}

func (t tenantLimits) TenantLimits(userID string) *validation.Limits {
	l, _ := t.source.Get(userID)
	return l
}

func (t tenantLimits) AllByUserID() map[string]*validation.Limits {
	return t.source.All()
}

// PushGate returns the push gate for the configured enforce state, or nil when
// the state is off. The gate reports whether a tenant's pushes must be denied
// and why. Only a tenant published as soft_locked is ever denied, and only in
// mode on. In mode shadow the decision is counted and the push is allowed.
func (s *Store) PushGate() func(tenantID string) (deny bool, reason string) {
	if s.enforceMode == xstenant.EnforceOff {
		return nil
	}
	return func(tenantID string) (bool, string) {
		return s.reader.CheckPush(tenantID, s.enforceMode)
	}
}

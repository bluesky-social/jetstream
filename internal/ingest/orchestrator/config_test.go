package orchestrator

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	localcatalog "github.com/bluesky-social/jetstream/internal/catalog/local"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/stretchr/testify/require"
)

// validBaseConfig returns the minimal Config that passes validate.
// Tests mutate one field at a time off this baseline to assert
// per-field requirements.
func validBaseConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	st, err := pebblestore.Open(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	return Config{
		DataDir:    dir,
		Store:      st,
		RelayURL:   "https://relay.example",
		HTTPClient: &http.Client{},
		Directory:  &identity.Directory{},
		Verifier:   &atmossync.Verifier{},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestConfig_Validate_NegativeMergeDiscoveryRetryBaseDelay(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig(t)
	cfg.MergeDiscoveryRetryBaseDelay = -time.Nanosecond
	err := cfg.validate()
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "MergeDiscoveryRetryBaseDelay")
}

func TestConfig_Validate_OK(t *testing.T) {
	t.Parallel()
	cfg := validBaseConfig(t)
	require.NoError(t, cfg.validate())
}

// TestConfig_Validate_MissingFields exercises a representative
// missing field. The validate body is straight-line and adding a
// case per field would be noise; this anchors the
// ErrInvalidConfig-wrapped, named-field-cited contract.
func TestConfig_Validate_MissingFields(t *testing.T) {
	t.Parallel()

	cfg := validBaseConfig(t)
	cfg.Store = nil
	err := cfg.validate()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidConfig)
	require.Contains(t, err.Error(), "Store")
}

func TestConfig_Validate_Disaggregated(t *testing.T) {
	t.Parallel()
	valid := func() Config {
		cfg := validBaseConfig(t)
		cfg.DataDir = ""
		cfg.Disaggregated = &Disaggregated{
			Session: &catalog.Session{},
			Direct: func(context.Context, catalog.Namespace) (*ingest.DirectConfig, error) {
				return nil, nil
			},
			Hot:     func(context.Context) (*ingest.HotConfig, error) { return nil, nil },
			Objects: nopObjects{},
		}
		return cfg
	}
	cfg := valid()
	require.NoError(t, cfg.validate())

	for name, mutate := range map[string]func(*Config){
		"no session":            func(c *Config) { c.Disaggregated.Session = nil },
		"no direct":             func(c *Config) { c.Disaggregated.Direct = nil },
		"no hot":                func(c *Config) { c.Disaggregated.Hot = nil },
		"no objects":            func(c *Config) { c.Disaggregated.Objects = nil },
		"compaction":            func(c *Config) { c.CompactionInterval = time.Minute },
		"async flush":           func(c *Config) { c.BackfillAsyncFlushWorkers = 2 },
		"local segment catalog": func(c *Config) { c.Catalog = &localcatalog.Catalog{} },
	} {
		cfg := valid()
		mutate(&cfg)
		require.ErrorIs(t, cfg.validate(), ErrInvalidConfig, name)
	}
}

// nopObjects is an objstore.Store for config validation only.
type nopObjects struct{ objstore.Store }

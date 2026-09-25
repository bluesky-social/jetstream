package orchestrator

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
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

func TestConfig_Validate_Hot(t *testing.T) {
	t.Parallel()
	cfg := validBaseConfig(t)
	cfg.Hot, cfg.DataDir = &ingest.HotConfig{}, ""
	require.NoError(t, cfg.validate())

	cfg.CompactionInterval = time.Minute
	require.ErrorIs(t, cfg.validate(), ErrInvalidConfig)
}

// TestRun_HotRefusesNonSteadyPhase pins that disaggregated mode never starts
// bootstrap or merge, and never writes the initial phase itself.
func TestRun_HotRefusesNonSteadyPhase(t *testing.T) {
	t.Parallel()
	for _, phase := range []lifecycle.Phase{"", lifecycle.PhaseBootstrap, lifecycle.PhaseMerging} {
		t.Run(string(phase), func(t *testing.T) {
			t.Parallel()
			cfg := validBaseConfig(t)
			cfg.Hot = &ingest.HotConfig{}
			if phase != "" {
				require.NoError(t, lifecycle.WritePhase(t.Context(), cfg.Store, phase, time.Now()))
			}
			o, err := New(cfg)
			require.NoError(t, err)
			err = o.Run(t.Context())
			require.ErrorContains(t, err, "disaggregated mode needs phase")

			got, err := lifecycle.ReadPhase(t.Context(), cfg.Store)
			require.NoError(t, err)
			require.Equal(t, phase, got)
		})
	}
}

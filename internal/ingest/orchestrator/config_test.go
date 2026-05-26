package orchestrator

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
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
	st, err := store.Open(dir, nil)
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

func TestConfig_OnEventField(t *testing.T) {
	t.Parallel()
	// Compile-time assertion: Config has the OnEvent field with the
	// expected signature. The field flows through into the steady-state
	// live.Consumer; see runSteadyState in steady.go.
	cfg := Config{
		OnEvent: func(*segment.Event) {},
	}
	require.NotNil(t, cfg.OnEvent)
}

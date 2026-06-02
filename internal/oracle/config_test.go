package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigDefaultMode(t *testing.T) {
	t.Parallel()

	cfg := ConfigFromEnv(func(string) string { return "" })
	require.Equal(t, "default", cfg.Mode)
	require.Equal(t, 25, cfg.Accounts)
	require.Equal(t, 0, cfg.MinInitialRecords)
	require.Equal(t, 1000, cfg.MaxInitialRecords)
	require.Greater(t, cfg.LiveEventsBootstrap, 0)
	require.Greater(t, cfg.LiveEventsSteady, 0)
}

func TestConfigEnvOverrides(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"JETSTREAM_ORACLE_MODE":                "fast",
		"JETSTREAM_ORACLE_SEED":                "99",
		"JETSTREAM_ORACLE_ACCOUNTS":            "7",
		"JETSTREAM_ORACLE_MAX_INITIAL_RECORDS": "13",
	}
	cfg := ConfigFromEnv(func(k string) string { return env[k] })
	require.Equal(t, "fast", cfg.Mode)
	require.Equal(t, uint64(99), cfg.Seed)
	require.Equal(t, 7, cfg.Accounts)
	require.Equal(t, 13, cfg.MaxInitialRecords)
}

func TestConfigModePresets(t *testing.T) {
	t.Parallel()

	fast, err := ParseConfigFromEnv(envMap(map[string]string{
		"JETSTREAM_ORACLE_MODE": "fast",
	}))
	require.NoError(t, err)
	require.Equal(t, 8, fast.Accounts)
	require.Equal(t, 0, fast.MinInitialRecords)
	require.Equal(t, 50, fast.MaxInitialRecords)
	require.Equal(t, 25, fast.LiveEventsBootstrap)
	require.Equal(t, 25, fast.LiveEventsSteady)

	stress, err := ParseConfigFromEnv(envMap(map[string]string{
		"JETSTREAM_ORACLE_MODE": "stress",
	}))
	require.NoError(t, err)
	require.Equal(t, 100, stress.Accounts)
	require.Equal(t, 0, stress.MinInitialRecords)
	require.Equal(t, 5000, stress.MaxInitialRecords)
	require.Equal(t, 5000, stress.LiveEventsBootstrap)
	require.Equal(t, 5000, stress.LiveEventsSteady)
}

func TestConfigRejectsInvalidEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "unknown mode",
			env:     map[string]string{"JETSTREAM_ORACLE_MODE": "tiny"},
			wantErr: "unknown oracle mode",
		},
		{
			name:    "invalid seed",
			env:     map[string]string{"JETSTREAM_ORACLE_SEED": "nope"},
			wantErr: "JETSTREAM_ORACLE_SEED",
		},
		{
			name:    "negative seed",
			env:     map[string]string{"JETSTREAM_ORACLE_SEED": "-1"},
			wantErr: "JETSTREAM_ORACLE_SEED",
		},
		{
			name:    "invalid accounts",
			env:     map[string]string{"JETSTREAM_ORACLE_ACCOUNTS": "nope"},
			wantErr: "JETSTREAM_ORACLE_ACCOUNTS",
		},
		{
			name:    "zero accounts",
			env:     map[string]string{"JETSTREAM_ORACLE_ACCOUNTS": "0"},
			wantErr: "JETSTREAM_ORACLE_ACCOUNTS must be positive",
		},
		{
			name:    "negative min records",
			env:     map[string]string{"JETSTREAM_ORACLE_MIN_INITIAL_RECORDS": "-1"},
			wantErr: "JETSTREAM_ORACLE_MIN_INITIAL_RECORDS must be non-negative",
		},
		{
			name: "max before min",
			env: map[string]string{
				"JETSTREAM_ORACLE_MIN_INITIAL_RECORDS": "11",
				"JETSTREAM_ORACLE_MAX_INITIAL_RECORDS": "10",
			},
			wantErr: "JETSTREAM_ORACLE_MAX_INITIAL_RECORDS must be >= JETSTREAM_ORACLE_MIN_INITIAL_RECORDS",
		},
		{
			name:    "zero live bootstrap",
			env:     map[string]string{"JETSTREAM_ORACLE_LIVE_EVENTS_BOOTSTRAP": "0"},
			wantErr: "JETSTREAM_ORACLE_LIVE_EVENTS_BOOTSTRAP must be positive",
		},
		{
			name:    "zero live steady",
			env:     map[string]string{"JETSTREAM_ORACLE_LIVE_EVENTS_STEADY": "0"},
			wantErr: "JETSTREAM_ORACLE_LIVE_EVENTS_STEADY must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseConfigFromEnv(envMap(tt.env))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfigRejectsExplicitEmptyEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name:    "empty mode",
			env:     map[string]string{"JETSTREAM_ORACLE_MODE": ""},
			wantErr: "JETSTREAM_ORACLE_MODE must not be empty",
		},
		{
			name:    "empty numeric override",
			env:     map[string]string{"JETSTREAM_ORACLE_ACCOUNTS": ""},
			wantErr: "JETSTREAM_ORACLE_ACCOUNTS must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseConfigFromLookupEnv(lookupEnvMap(tt.env))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestConfigFromEnvPanicsOnInvalidEnv(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		ConfigFromEnv(envMap(map[string]string{
			"JETSTREAM_ORACLE_MODE": "tiny",
		}))
	})
}

func envMap(env map[string]string) func(string) string {
	return func(k string) string {
		return env[k]
	}
}

func lookupEnvMap(env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := env[k]
		return v, ok
	}
}

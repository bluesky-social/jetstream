package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigEnvOverrides(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"JETSTREAM_ORACLE_MODE":                "fast",
		"JETSTREAM_ORACLE_SEED":                "99",
		"JETSTREAM_ORACLE_ACCOUNTS":            "7",
		"JETSTREAM_ORACLE_MAX_INITIAL_RECORDS": "13",
		"JETSTREAM_ORACLE_FAULT_MODE":          "none",
	}
	cfg := ConfigFromEnv(func(k string) string { return env[k] })
	require.Equal(t, "fast", cfg.Mode)
	require.Equal(t, uint64(99), cfg.Seed)
	require.Equal(t, 7, cfg.Accounts)
	require.Equal(t, 13, cfg.MaxInitialRecords)
	// Swarm is the default; verify the env var can still opt back out.
	require.Equal(t, FaultModeNone, cfg.FaultMode)
}

func TestDefaultLifecycleConfigUsesFastModeUnderShort(t *testing.T) {
	t.Parallel()

	cfg, err := defaultLifecycleConfig(lookupEnvMap(nil), true)
	require.NoError(t, err)
	require.Equal(t, "fast", cfg.Mode)
	require.Equal(t, 8, cfg.Accounts)
	require.Equal(t, 10, cfg.MaxInitialRecords)
}

func TestDefaultLifecycleConfigHonorsExplicitModeUnderShort(t *testing.T) {
	t.Parallel()

	cfg, err := defaultLifecycleConfig(lookupEnvMap(map[string]string{
		"JETSTREAM_ORACLE_MODE": "default",
	}), true)
	require.NoError(t, err)
	require.Equal(t, "default", cfg.Mode)
	require.Equal(t, 25, cfg.Accounts)
	require.Equal(t, 1000, cfg.MaxInitialRecords)
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
			name:    "unknown fault mode",
			env:     map[string]string{"JETSTREAM_ORACLE_FAULT_MODE": "tiny"},
			wantErr: "unknown oracle fault mode",
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
			name:    "empty fault mode",
			env:     map[string]string{"JETSTREAM_ORACLE_FAULT_MODE": ""},
			wantErr: "JETSTREAM_ORACLE_FAULT_MODE must not be empty",
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

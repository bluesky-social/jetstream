package oracle

import (
	"fmt"
	"strconv"
)

const (
	envOracleMode                = "JETSTREAM_ORACLE_MODE"
	envOracleSeed                = "JETSTREAM_ORACLE_SEED"
	envOracleAccounts            = "JETSTREAM_ORACLE_ACCOUNTS"
	envOracleMinInitialRecords   = "JETSTREAM_ORACLE_MIN_INITIAL_RECORDS"
	envOracleMaxInitialRecords   = "JETSTREAM_ORACLE_MAX_INITIAL_RECORDS"
	envOracleLiveEventsBootstrap = "JETSTREAM_ORACLE_LIVE_EVENTS_BOOTSTRAP"
	envOracleLiveEventsSteady    = "JETSTREAM_ORACLE_LIVE_EVENTS_STEADY"
	envOracleFaultMode           = "JETSTREAM_ORACLE_FAULT_MODE"
)

// FaultModeNone and FaultModeSwarm are the supported values for Config.FaultMode:
// no injected faults, or the bounded transient-getRepo fault swarm.
const (
	FaultModeNone  = "none"
	FaultModeSwarm = "swarm"
)

// Config holds the tunable parameters of an oracle harness run: the mode,
// RNG seed, account and record counts, live-event budgets, and fault mode.
type Config struct {
	Mode                string
	Seed                uint64
	Accounts            int
	MinInitialRecords   int
	MaxInitialRecords   int
	LiveEventsBootstrap int
	LiveEventsSteady    int
	FaultMode           string
}

// ConfigFromEnv returns oracle harness configuration and panics on invalid
// environment values. Use ParseConfigFromEnv when callers can surface errors.
func ConfigFromEnv(getenv func(string) string) Config {
	cfg, err := ParseConfigFromEnv(getenv)
	if err != nil {
		panic(err)
	}
	return cfg
}

// ParseConfigFromLookupEnv is like ParseConfigFromEnv, but can distinguish
// absent variables from explicitly-empty variables. Callers using os.LookupEnv
// should prefer this so malformed empty overrides fail loudly.
func ParseConfigFromLookupEnv(lookupenv func(string) (string, bool)) (Config, error) {
	return parseConfigFromLookupEnv(lookupenv)
}

// ParseConfigFromEnv parses oracle configuration from a getenv-style lookup,
// treating empty values as absent. Use ParseConfigFromLookupEnv to distinguish
// unset variables from explicitly-empty ones.
func ParseConfigFromEnv(getenv func(string) string) (Config, error) {
	return parseConfigFromLookupEnv(func(key string) (string, bool) {
		value := getenv(key)
		return value, value != ""
	})
}

func parseConfigFromLookupEnv(lookupenv func(string) (string, bool)) (Config, error) {
	cfg := defaultConfig()

	mode, ok := lookupenv(envOracleMode)
	if !ok {
		mode = cfg.Mode
	} else if mode == "" {
		return Config{}, fmt.Errorf("%s must not be empty", envOracleMode)
	}
	switch mode {
	case "default":
	case "fast":
		cfg.Accounts = 8
		cfg.MaxInitialRecords = 10
		cfg.LiveEventsBootstrap = 12
		cfg.LiveEventsSteady = 12
	case "stress":
		cfg.Accounts = 100
		cfg.MaxInitialRecords = 5000
		cfg.LiveEventsBootstrap = 5000
		cfg.LiveEventsSteady = 5000
	default:
		return Config{}, fmt.Errorf("%s: unknown oracle mode %q", envOracleMode, mode)
	}
	cfg.Mode = mode

	faultMode, ok := lookupenv(envOracleFaultMode)
	if !ok {
		faultMode = cfg.FaultMode
	} else if faultMode == "" {
		return Config{}, fmt.Errorf("%s must not be empty", envOracleFaultMode)
	}
	switch faultMode {
	case FaultModeNone, FaultModeSwarm:
	default:
		return Config{}, fmt.Errorf("%s: unknown oracle fault mode %q", envOracleFaultMode, faultMode)
	}
	cfg.FaultMode = faultMode

	if err := parseUint64Env(lookupenv, envOracleSeed, &cfg.Seed); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookupenv, envOracleAccounts, &cfg.Accounts); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookupenv, envOracleMinInitialRecords, &cfg.MinInitialRecords); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookupenv, envOracleMaxInitialRecords, &cfg.MaxInitialRecords); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookupenv, envOracleLiveEventsBootstrap, &cfg.LiveEventsBootstrap); err != nil {
		return Config{}, err
	}
	if err := parseIntEnv(lookupenv, envOracleLiveEventsSteady, &cfg.LiveEventsSteady); err != nil {
		return Config{}, err
	}

	if cfg.Accounts <= 0 {
		return Config{}, fmt.Errorf("%s must be positive, got %d", envOracleAccounts, cfg.Accounts)
	}
	if cfg.MinInitialRecords < 0 {
		return Config{}, fmt.Errorf("%s must be non-negative, got %d", envOracleMinInitialRecords, cfg.MinInitialRecords)
	}
	if cfg.MaxInitialRecords < 0 {
		return Config{}, fmt.Errorf("%s must be non-negative, got %d", envOracleMaxInitialRecords, cfg.MaxInitialRecords)
	}
	if cfg.MaxInitialRecords < cfg.MinInitialRecords {
		return Config{}, fmt.Errorf("%s must be >= %s, got %d < %d", envOracleMaxInitialRecords, envOracleMinInitialRecords, cfg.MaxInitialRecords, cfg.MinInitialRecords)
	}
	if cfg.LiveEventsBootstrap <= 0 {
		return Config{}, fmt.Errorf("%s must be positive, got %d", envOracleLiveEventsBootstrap, cfg.LiveEventsBootstrap)
	}
	if cfg.LiveEventsSteady <= 0 {
		return Config{}, fmt.Errorf("%s must be positive, got %d", envOracleLiveEventsSteady, cfg.LiveEventsSteady)
	}

	return cfg, nil
}

func defaultConfig() Config {
	return Config{
		Mode:                "default",
		Seed:                42,
		Accounts:            25,
		MinInitialRecords:   0,
		MaxInitialRecords:   1000,
		LiveEventsBootstrap: 200,
		LiveEventsSteady:    200,
		// Swarm fault injection is on by default so the nightly oracle
		// sweep exercises the transient-getRepo recovery path on every
		// run. Set JETSTREAM_ORACLE_FAULT_MODE=none to opt out (e.g. to
		// isolate a non-fault regression). The fault budget is bounded
		// and transient, so the durable model still matches the
		// simulator world; see internal/oracle/faults.go.
		FaultMode: FaultModeSwarm,
	}
}

func parseUint64Env(lookupenv func(string) (string, bool), key string, out *uint64) error {
	value, ok := lookupenv(key)
	if !ok {
		return nil
	}
	if value == "" {
		return fmt.Errorf("%s must not be empty", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: parse uint64 %q: %w", key, value, err)
	}
	*out = parsed
	return nil
}

func parseIntEnv(lookupenv func(string) (string, bool), key string, out *int) error {
	value, ok := lookupenv(key)
	if !ok {
		return nil
	}
	if value == "" {
		return fmt.Errorf("%s must not be empty", key)
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("%s: parse int %q: %w", key, value, err)
	}
	*out = parsed
	return nil
}

func parseInt64Env(lookupenv func(string) (string, bool), key string, out *int64) error {
	value, ok := lookupenv(key)
	if !ok {
		return nil
	}
	if value == "" {
		return fmt.Errorf("%s must not be empty", key)
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("%s: parse int64 %q: %w", key, value, err)
	}
	*out = parsed
	return nil
}

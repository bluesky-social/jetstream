package obs

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// LogFormat selects the slog handler implementation.
type LogFormat string

const (
	LogFormatText LogFormat = "text"
	LogFormatJSON LogFormat = "json"
)

// ParseLogLevel maps a case-insensitive name to slog.Level.
// We accept the four canonical levels; anything else is a configuration
// error and we surface it loudly rather than defaulting silently.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "d", "dbg", "debug":
		return slog.LevelDebug, nil
	case "i", "info", "":
		return slog.LevelInfo, nil
	case "w", "warn", "warning":
		return slog.LevelWarn, nil
	case "e", "err", "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug|info|warn|error)", s)
	}
}

// ParseLogFormat validates a format name.
//
// The empty string maps to LogFormatJSON to match the CLI flag's
// default. Without that mapping, users clearing JETSTREAM_LOG_FORMAT
// (e.g. setting the env var to "") would silently land on text
// format, contradicting both the documented default and what they
// see in `--help`.
func ParseLogFormat(s string) (LogFormat, error) {
	switch LogFormat(strings.ToLower(strings.TrimSpace(s))) {
	case "":
		return LogFormatJSON, nil
	case LogFormatText:
		return LogFormatText, nil
	case LogFormatJSON:
		return LogFormatJSON, nil
	default:
		return "", fmt.Errorf("unknown log format %q (want text|json)", s)
	}
}

// NewLogger constructs a slog.Logger that writes to w at the given level
// using the chosen handler format.
func NewLogger(w io.Writer, level slog.Level, format LogFormat) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler
	switch format {
	case LogFormatJSON:
		h = slog.NewJSONHandler(w, opts)
	default:
		h = slog.NewTextHandler(w, opts)
	}

	return slog.New(h)
}

// BuildLoggerFromStrings parses level and format strings (typically
// from CLI flags / env vars) and returns a logger that writes to w.
// Centralizes the parse-and-construct flow so every subcommand wires
// logging the same way.
func BuildLoggerFromStrings(w io.Writer, levelStr, formatStr string) (*slog.Logger, error) {
	level, err := ParseLogLevel(levelStr)
	if err != nil {
		return nil, err
	}
	format, err := ParseLogFormat(formatStr)
	if err != nil {
		return nil, err
	}
	return NewLogger(w, level, format), nil
}

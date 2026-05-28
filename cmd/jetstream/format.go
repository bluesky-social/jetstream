package main

import (
	"fmt"
	"io"
	"time"
)

// formatMicros formats a unix-microsecond timestamp as RFC3339 with
// six-digit fractional seconds in UTC. Zero -> the literal "0" so the
// renderer doesn't print a misleading 1970 timestamp on a fresh file.
func formatMicros(us int64) string {
	if us == 0 {
		return "0"
	}
	t := time.UnixMicro(us).UTC()
	return t.Format("2006-01-02T15:04:05.000000Z")
}

// errWriter accumulates a write error so renderers can be a sequence
// of bw.printf calls without an `if err != nil` after every one. The
// first error is sticky; subsequent writes are dropped.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

// formatTime renders t in the same RFC3339-micros UTC format used by
// formatMicros. Zero time renders as "0" so a freshly initialized
// aggregate (no records yet) doesn't print a misleading 1970 stamp.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}

// humanInt renders n with comma separators ("1,234,567").
func humanInt(n uint64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result string
	pre := len(s) % 3
	if pre > 0 {
		result = s[:pre]
		if len(s) > pre {
			result += ","
		}
	}
	for i := pre; i < len(s); i += 3 {
		result += s[i : i+3]
		if i+3 < len(s) {
			result += ","
		}
	}
	return result
}

// formatBytes formats n as a base-1024 human-readable size.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffixes[exp])
}

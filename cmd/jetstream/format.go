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

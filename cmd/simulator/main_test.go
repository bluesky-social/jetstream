package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLogMsgSet pins the E2E warning-sentinel log parser (issue #283).
// The heavy E2E test keys its verifier-overflow and unknown-event
// sentinels off the structured slog `msg` field this returns, so a
// regression here would silently disable those sentinels.
func TestLogMsgSet(t *testing.T) {
	t.Parallel()

	t.Run("extracts distinct msg values from JSON slog lines", func(t *testing.T) {
		t.Parallel()
		logs := `{"time":"t","level":"WARN","msg":"verify queue overflow dropped event","did":"did:plc:x","seq":42,"queue_len":64}
{"time":"t","level":"WARN","msg":"verification failure","did":"did:plc:x","err":"chain break"}
{"time":"t","level":"INFO","msg":"steady state reached"}
{"time":"t","level":"INFO","msg":"steady state reached"}`
		msgs := logMsgSet(logs)
		require.Len(t, msgs, 3)
		require.Contains(t, msgs, "verify queue overflow dropped event")
		require.Contains(t, msgs, "verification failure")
		require.Contains(t, msgs, "steady state reached")
	})

	t.Run("the drop message is a distinct msg, not a substring of atmos text", func(t *testing.T) {
		t.Parallel()
		// Regression guard for #283: the old sentinel scanned for the
		// substring "event dropped", which jetstream's message
		// ("verify queue overflow dropped event") does not contain.
		// Confirm we match the whole message the consumer actually
		// emits, and that we do NOT spuriously key off field values.
		logs := `{"time":"t","level":"WARN","msg":"verify queue overflow dropped event","did":"did:plc:x","seq":42}`
		msgs := logMsgSet(logs)
		_, dropOccurred := msgs["verify queue overflow dropped event"]
		require.True(t, dropOccurred, "must recognize jetstream's own drop message")
		_, atmosText := msgs["event dropped"]
		require.False(t, atmosText, "atmos's raw DropError text never reaches the log buffer")
	})

	t.Run("ignores non-JSON and blank lines", func(t *testing.T) {
		t.Parallel()
		logs := "panic: runtime error\n\ngoroutine 1 [running]:\n{\"level\":\"WARN\",\"msg\":\"unknown event kind\",\"kind\":9}\n"
		msgs := logMsgSet(logs)
		require.Len(t, msgs, 1)
		require.Contains(t, msgs, "unknown event kind")
	})

	t.Run("empty input yields empty set", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, logMsgSet(""))
	})
}

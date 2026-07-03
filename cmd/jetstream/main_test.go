package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewApp_VersionCommandPrintsToStdout verifies that the `version`
// subcommand writes to the command's Writer (which defaults to stdout) and
// includes the build metadata. We retarget Writer to a buffer so the
// assertion doesn't depend on stdout interception.
func TestNewApp_VersionCommandPrintsToStdout(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	app := newTestApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{"jetstream", "version"})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "jetstream version")
	require.Contains(t, out, "commit")
	require.Contains(t, out, "built")
}

func TestNewApp_InspectSegmentRunsAgainstSealedFile(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	var buf bytes.Buffer
	app := newTestApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{"jetstream", "inspect-segment", path})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, path)
}

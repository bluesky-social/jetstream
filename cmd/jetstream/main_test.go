package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewApp_VersionFlagDoesNotError(t *testing.T) {
	t.Parallel()

	// --version is handled by urfave/cli internally and returns nil.
	// This exercises the wiring without invoking the network-bound serve action.
	err := newApp().Run(t.Context(), []string{"jetstream", "--version"})
	require.NoError(t, err)
}

func TestNewApp_HelpDoesNotError(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "--help"})
	require.NoError(t, err)
}

func TestNewApp_ServeHelpDoesNotError(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "serve", "--help"})
	require.NoError(t, err)
}

// TestNewApp_VersionCommandPrintsToStdout verifies that the `version`
// subcommand writes to the command's Writer (which defaults to stdout) and
// includes the build metadata. We retarget Writer to a buffer so the
// assertion doesn't depend on stdout interception.
func TestNewApp_VersionCommandPrintsToStdout(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{"jetstream", "version"})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "jetstream version")
	require.Contains(t, out, "commit")
	require.Contains(t, out, "built")
}

func TestNewApp_InspectSegmentHelpDoesNotError(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "inspect-segment", "--help"})
	require.NoError(t, err)
}

func TestNewApp_InspectSegmentRunsAgainstSealedFile(t *testing.T) {
	t.Parallel()

	path := makeSealedFixture(t)

	var buf bytes.Buffer
	app := newApp()
	app.Writer = &buf

	err := app.Run(t.Context(), []string{"jetstream", "inspect-segment", path})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "state: sealed")
	require.Contains(t, out, path)
}

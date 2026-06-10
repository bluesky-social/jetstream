package main

import (
	"bytes"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
	"github.com/stretchr/testify/require"
)

func TestNewApp_DoesNotRegisterExportRepoCommand(t *testing.T) {
	t.Parallel()

	for _, cmd := range newApp().Commands {
		require.NotEqual(t, "export-repo", cmd.Name)
	}
}

func TestVerifyRepoCommandRequiresExactlyOneDID(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "verify-repo"})
	require.Error(t, err)
	require.ErrorContains(t, err, "expected exactly one DID")
}

func TestRenderRepoVerifyReportMatch(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := renderRepoVerifyReport(&buf, repoexport.VerifyReport{
		DID:               "did:plc:repo",
		Match:             true,
		AuthoritativeRev:  "rev2",
		AuthoritativeRoot: "bafyroot",
		LocalLatestRev:    "rev2",
		LocalRoot:         "bafyroot",
		LocalRecordCount:  12,
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "repo verification: match")
}

func TestRenderRepoVerifyReportMismatch(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := renderRepoVerifyReport(&buf, repoexport.VerifyReport{
		DID:               "did:plc:repo",
		Match:             false,
		AuthoritativeRev:  "rev2",
		AuthoritativeRoot: "bafyauthoritative",
		LocalLatestRev:    "rev1",
		LocalRoot:         "bafylocal",
		LocalRecordCount:  11,
		Message:           "local reconstructed MST root does not match authoritative repo root",
	})
	require.ErrorIs(t, err, errRepoVerifyMismatch)

	out := buf.String()
	require.Contains(t, out, "repo verification: mismatch")
	require.Contains(t, out, "authoritative_root:")
	require.Contains(t, out, "local_root:")
}

func TestNewApp_VerifyRepoHelpDoesNotError(t *testing.T) {
	t.Parallel()

	err := newApp().Run(t.Context(), []string{"jetstream", "verify-repo", "--help"})
	require.NoError(t, err)
}

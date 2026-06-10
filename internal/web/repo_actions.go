package web

import (
	"context"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
)

type repoExportActions struct {
	dataDir  string
	relayURL string
}

// NewRepoActions builds the production repo action implementation used by the
// status handler.
func NewRepoActions(dataDir, relayURL string) RepoActions {
	return repoExportActions{
		dataDir:  dataDir,
		relayURL: relayURL,
	}
}

func (a repoExportActions) VerifyRepo(ctx context.Context, did string) (repoexport.VerifyReport, error) {
	return repoexport.Verify(ctx, repoexport.VerifyConfig{
		DataDir:  a.dataDir,
		DID:      did,
		RelayURL: a.relayURL,
	})
}

package repoexport

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

const (
	defaultVerifyRelayURL = "https://bsky.network"
	rootMismatchMessage   = "local reconstructed MST root does not match authoritative repo root"
)

// VerifyConfig controls authoritative-vs-local repo root verification.
type VerifyConfig struct {
	DataDir  string
	DID      string
	RelayURL string

	// Selector prunes which segments/blocks reconstruction decodes via the
	// in-memory manifest blooms. Required; see Config.Selector.
	Selector Selector

	// PendingEvents are the live writer's not-yet-flushed events, folded
	// into the local reconstruction so a record created moments ago is
	// reflected before the next compaction flush. See Config.PendingEvents.
	PendingEvents []segment.Event
}

// VerifyReport describes the outcome of comparing a local reconstruction
// against an authoritative repo CAR.
type VerifyReport struct {
	DID               string
	Match             bool
	AuthoritativeRev  string
	AuthoritativeRoot string
	LocalLatestRev    string
	LocalRoot         string
	LocalRecordCount  int
	Message           string
}

// Verify downloads cfg.DID's authoritative repo CAR and compares its commit
// MST root against a locally reconstructed snapshot.
func Verify(ctx context.Context, cfg VerifyConfig) (VerifyReport, error) {
	if cfg.DataDir == "" {
		return VerifyReport{}, errors.New("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return VerifyReport{}, errors.New("repoexport: DID is required")
	}
	if err := ctx.Err(); err != nil {
		return VerifyReport{}, err
	}

	relayURL := cfg.RelayURL
	if relayURL == "" {
		relayURL = defaultVerifyRelayURL
	}

	authoritativeRev, authoritativeRoot, err := loadAuthoritativeRoot(ctx, relayURL, cfg.DID)
	if err != nil {
		return VerifyReport{}, err
	}

	report := VerifyReport{
		DID:               cfg.DID,
		AuthoritativeRev:  authoritativeRev,
		AuthoritativeRoot: authoritativeRoot,
	}

	snap, err := Reconstruct(ctx, Config{
		DataDir:       cfg.DataDir,
		DID:           cfg.DID,
		Selector:      cfg.Selector,
		PendingEvents: cfg.PendingEvents,
	})
	if err != nil {
		if errors.Is(err, ErrNoLocalRepo) {
			report.Message = err.Error()
			return report, nil
		}
		return VerifyReport{}, err
	}

	report.LocalLatestRev = snap.LatestRev
	report.LocalRoot = snap.Root.String()
	report.LocalRecordCount = snap.RecordCount
	if authoritativeRoot == report.LocalRoot {
		report.Match = true
		return report, nil
	}

	report.Message = rootMismatchMessage
	return report, nil
}

func loadAuthoritativeRoot(ctx context.Context, relayURL, did string) (string, string, error) {
	xrpcClient := &xrpc.Client{
		Host:       relayURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	body, err := syncClient.GetRepoStream(ctx, atmos.DID(did), "")
	if err != nil {
		return "", "", fmt.Errorf("repoexport: download authoritative repo CAR for %s: %w", did, err)
	}
	defer func() { _ = body.Close() }()

	_, commit, err := repo.LoadFromCAR(body)
	if err != nil {
		return "", "", fmt.Errorf("repoexport: decode authoritative repo CAR for %s: %w", did, err)
	}
	if commit.DID != did {
		return "", "", fmt.Errorf("repoexport: authoritative repo CAR DID mismatch: requested %s, commit DID %s", did, commit.DID)
	}

	return commit.Rev, commit.Data.String(), nil
}

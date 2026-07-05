// Package orchestrator: merge_discovery.go runs the post-merge
// listRepos resume that picks up DIDs born during the bootstrap
// window. Spec:
// docs/superpowers/specs/2026-05-27-merge-phase-design.md §4.7.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/store"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
)

// listReposPageLimit matches the atmos default. Chosen to amortize
// HTTP overhead while keeping pagination latency bounded.
const listReposPageLimit int64 = 1000

// runDiscovery resumes listRepos from bootstrap/last_listrepos_cursor
// and writes a StatusFailed row for every previously-unknown DID.
// No-op when the cursor key is absent (debug short-circuit runs that
// never paged past page 1).
//
// Couples merge completion to relay availability: a relay outage at
// cutover time prevents merge from advancing. See spec §4.7 for the
// rationale and §9 for the documented future-work option of moving
// this to a separate post-steady-state-startup task.
func (r *mergeRunner) runDiscovery(ctx context.Context, relayURL string, httpClient *http.Client) error {
	return obs.Span(ctx, func(ctx context.Context) error {
		cursor, err := backfill.LoadBootstrapLastListReposCursor(r.store)
		if err != nil {
			return fmt.Errorf("orchestrator: merge: load bootstrap cursor: %w", err)
		}
		if cursor == "" {
			r.logger.InfoContext(ctx, "skipping discovery: no bootstrap-last cursor")
			return nil
		}

		xc := &xrpc.Client{
			Host:       relayURL,
			HTTPClient: gt.Some(httpClient),
			Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
		}
		sc := atmossync.NewClient(atmossync.Options{Client: xc})

		seenCursors := map[string]struct{}{cursor: {}}
		for page, perr := range sc.ListRepos(ctx, listReposPageLimit, cursor) {
			if perr != nil {
				return fmt.Errorf("orchestrator: merge: discovery listRepos: %w", perr)
			}
			for _, entry := range page.Entries {
				if err := r.maybeWriteDiscoveredRow(ctx, entry); err != nil {
					return err
				}
			}
			if page.NextCursor != "" {
				if _, ok := seenCursors[page.NextCursor]; ok {
					return fmt.Errorf("orchestrator: merge: discovery listRepos cursor loop at %q", page.NextCursor)
				}
				seenCursors[page.NextCursor] = struct{}{}
			}
		}
		return nil
	})
}

// maybeWriteDiscoveredRow writes a StatusFailed-shaped row only when
// the DID has no existing repo/<did> entry. Idempotent across reruns.
func (r *mergeRunner) maybeWriteDiscoveredRow(ctx context.Context, entry atmossync.ListReposEntry) error {
	did := string(entry.DID)
	_, closer, err := r.store.Get(backfill.RepoKey(did))
	if err == nil {
		_ = closer.Close()
		return nil // existing row; race-safe with bootstrap's tail
	}
	if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("orchestrator: merge: discovery lookup %s: %w", did, err)
	}

	rs := &backfill.RepoStatus{
		Backfill: backfill.RepoBackfillStatus{
			Status:    backfill.StatusFailed,
			LastError: "discovered post-bootstrap; queued for retry",
		},
		Active: entry.Active,
	}
	enc, err := backfill.EncodeRepoStatus(rs)
	if err != nil {
		return fmt.Errorf("orchestrator: merge: discovery encode %s: %w", did, err)
	}
	if err := r.store.Set(backfill.RepoKey(did), enc, store.SyncWrites); err != nil {
		return fmt.Errorf("orchestrator: merge: discovery write %s: %w", did, err)
	}
	r.metrics.incMergeDIDsDiscoveredPostBootstrap()
	r.logger.InfoContext(ctx, "discovered post-bootstrap DID", "did", did)
	return nil
}

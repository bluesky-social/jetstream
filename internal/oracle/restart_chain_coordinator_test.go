package oracle

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/stretchr/testify/require"
)

// chainCoordinator drives a seed-derived chain of durable intermediates
// across the parent/child process boundary (plan §3, Option A). It is
// installed as the simulator's OnGetRepoServed hook: when the child's
// backfill serves the chain DID's getRepo, that DID's head rev is pinned,
// so it is now safe to generate create/update/delete ops at a HIGHER rev.
// Those ops ride the live firehose to the child's bootstrap-live consumer
// and survive the merge rev-filter (rev > BackfillRev) as durable
// intermediates. Generation happens exactly once, on the FIRST getRepo
// for the chain DID.
type chainCoordinator struct {
	t          *testing.T
	w          *world.World
	spec       chainSpec
	hostDID    string
	didReactID string // DID hosting shape F, "" if none

	once sync.Once
	mu   sync.Mutex
	ops  []world.GeneratedChainOp // recorded in generation order
	err  error

	// shape F result, generated on the didReact DID's getRepo.
	didReactOnce sync.Once
	didReactErr  error
	didReactDone bool
}

func newChainCoordinator(t *testing.T, w *world.World, spec chainSpec) *chainCoordinator {
	t.Helper()
	acct, err := w.LoadAccount(spec.chainAccountIdx())
	require.NoError(t, err)
	c := &chainCoordinator{
		t:       t,
		w:       w,
		spec:    spec,
		hostDID: string(acct.DID),
	}
	if spec.didReact != nil {
		dacct, err := w.LoadAccount(spec.didReact.accountIdx)
		require.NoError(t, err)
		c.didReactID = string(dacct.DID)
	}
	c.seedBackfillRecords()
	return c
}

// seedBackfillRecords generates the R_bf seed creates BEFORE the child
// spawns, so they are captured by the child's getRepo snapshot at the
// repo head rev (becoming KindCreate backfill rows). The matching live
// op(s) are generated later in onGetRepoServed at a higher rev. Must be
// called before the restart child starts.
func (c *chainCoordinator) seedBackfillRecords() {
	c.t.Helper()
	ctx := context.Background()
	for _, rc := range c.spec.records {
		for _, action := range rc.backfillOps() {
			_, _, err := c.w.GenerateRecordOpForTest(ctx, rc.accountIdx, action, rc.collection, rc.rkey)
			require.NoErrorf(c.t, err, "seed backfill %s %s %s/%s", rc.shape, action, rc.collection, rc.rkey)
		}
	}
}

// onGetRepoServed is the simulator hook. It fires on every getRepo. The
// record chains are generated on the chain host's first getRepo; shape F
// is generated on the shape-F DID's first getRepo (each DID's head is
// pinned only once ITS own snapshot is served, so each fixture keys off
// its own DID).
func (c *chainCoordinator) onGetRepoServed(did string) {
	if did == c.hostDID {
		c.once.Do(func() {
			ops, err := c.generate()
			c.mu.Lock()
			c.ops = ops
			c.err = err
			c.mu.Unlock()
		})
	}
	if c.didReactID != "" && did == c.didReactID {
		c.didReactOnce.Do(func() {
			err := c.generateDIDReactivation()
			c.mu.Lock()
			c.didReactErr = err
			c.didReactDone = true
			c.mu.Unlock()
		})
	}
}

// generateDIDReactivation drives shape F on its dedicated DID: an
// account-delete (DID tombstone, above backfill), a reactivation, then a
// fresh post. All ride the live firehose; account frames are not
// rev-filtered by the merge, so the tombstone + reactivation land durably.
func (c *chainCoordinator) generateDIDReactivation() error {
	ctx := context.Background()
	f := c.spec.didReact
	if _, err := c.w.GenerateAccountDeleteForTest(ctx, f.accountIdx); err != nil {
		return fmt.Errorf("shape F account delete: %w", err)
	}
	if _, err := c.w.GenerateAccountReactivateForTest(ctx, f.accountIdx); err != nil {
		return fmt.Errorf("shape F reactivate: %w", err)
	}
	if _, _, err := c.w.GenerateRecordOpForTest(ctx, f.accountIdx, "create", f.collection, f.rkey); err != nil {
		return fmt.Errorf("shape F post-reactivation create: %w", err)
	}
	return nil
}

// didReactResult returns shape F's generation status, failing the test if
// it never ran or errored.
func (c *chainCoordinator) didReactResult() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NoError(c.t, c.didReactErr, "shape F generation failed")
	require.True(c.t, c.didReactDone, "shape F never generated: getRepo for its DID was not observed")
}

// generate issues every record chain's LIVE ops in order on the host DID
// (the R_bf seed creates were already generated pre-spawn). Each op is a
// real #commit on the live firehose at a fresh rev > the backfill head
// rev, so it survives the merge as a durable intermediate.
func (c *chainCoordinator) generate() ([]world.GeneratedChainOp, error) {
	ctx := context.Background()
	var out []world.GeneratedChainOp
	for _, rc := range c.spec.records {
		for _, action := range rc.liveOps() {
			_, op, err := c.w.GenerateRecordOpForTest(ctx, rc.accountIdx, action, rc.collection, rc.rkey)
			if err != nil {
				return out, fmt.Errorf("chain %s %s %s/%s: %w", rc.shape, action, rc.collection, rc.rkey, err)
			}
			out = append(out, op)
		}
	}
	return out, nil
}

// recordedOps returns the generated chain ops, failing the test if
// generation never ran or errored.
func (c *chainCoordinator) recordedOps() []world.GeneratedChainOp {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NoError(c.t, c.err, "chain generation failed")
	require.NotEmpty(c.t, c.ops, "chain never generated: getRepo for the chain DID was not observed")
	return c.ops
}

// readCompactionWatermark opens the child's metadata store read-only
// (post-exit) and returns the merge-tail compaction watermark W. A
// missing key means no pass ran → W=0 (overlay covers everything).
func readCompactionWatermark(t *testing.T, dataDir string) uint64 {
	t.Helper()
	st, err := store.Open(dataDir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, st.Close()) }()
	w, ok, err := st.GetVersionedUint64LE("compaction/seq", 0x01)
	require.NoError(t, err)
	if !ok {
		return 0
	}
	return w
}

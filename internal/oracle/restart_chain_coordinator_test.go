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
	t       *testing.T
	w       *world.World
	spec    chainSpec
	hostDID string

	once sync.Once
	mu   sync.Mutex
	ops  []world.GeneratedChainOp // recorded in generation order
	err  error
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

// onGetRepoServed is the simulator hook. It fires on every getRepo; we
// generate the chain only the first time the chain DID is served.
func (c *chainCoordinator) onGetRepoServed(did string) {
	if did != c.hostDID {
		return
	}
	c.once.Do(func() {
		ops, err := c.generate()
		c.mu.Lock()
		c.ops = ops
		c.err = err
		c.mu.Unlock()
	})
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

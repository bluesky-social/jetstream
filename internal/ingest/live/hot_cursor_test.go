package live

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/coder/websocket"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/mst"
	atmosrepo "github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// cursorFirehose serves frames after the requested cursor, as a relay does,
// and records every requested cursor.
type cursorFirehose struct {
	t      *testing.T
	seqs   []int64
	frames [][]byte

	mu      sync.Mutex
	cursors []int64
}

func (f *cursorFirehose) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/com.atproto.sync.subscribeRepos") {
			http.NotFound(w, r)
			return
		}
		var cursor int64
		if q := r.URL.Query().Get("cursor"); q != "" {
			var err error
			if cursor, err = strconv.ParseInt(q, 10, 64); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		f.mu.Lock()
		f.cursors = append(f.cursors, cursor)
		f.mu.Unlock()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			f.t.Logf("firehose accept: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for i, frame := range f.frames {
			if f.seqs[i] <= cursor {
				continue
			}
			if err := conn.Write(r.Context(), websocket.MessageBinary, frame); err != nil {
				return
			}
		}
		<-r.Context().Done()
	})
}

func (f *cursorFirehose) requested() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.cursors...)
}

// buildMultiOpCommit is buildSyntheticChainedCommit with one create per rkey
// in a single signed commit. It also returns the rows the commit archives.
func buildMultiOpCommit(t *testing.T, r *atmosrepo.Repo, key crypto.PrivateKey, prevData cbor.CID, coll string, rkeys ...string) (*comatproto.SyncSubscribeRepos_Commit, []segment.Event) {
	t.Helper()
	var ops []comatproto.SyncSubscribeRepos_RepoOp
	var rows []segment.Event
	for _, rkey := range rkeys {
		require.NoError(t, r.Create(coll, rkey, map[string]any{"text": rkey}))
		cid, rec, err := r.Get(coll, rkey)
		require.NoError(t, err)
		ops = append(ops, comatproto.SyncSubscribeRepos_RepoOp{
			Action: "create",
			Path:   coll + "/" + rkey,
			CID:    gt.Some(lextypes.LexCIDLink{Link: cid.String()}),
		})
		rows = append(rows, segment.Event{
			Kind: segment.KindCreate, DID: string(r.DID), Collection: coll, Rkey: rkey,
			Payload: append([]byte(nil), rec...),
		})
	}
	postRoot, err := r.Tree.WriteBlocks(r.Store)
	require.NoError(t, err)
	rev := r.Clock.Next()
	c := &atmosrepo.Commit{DID: string(r.DID), Version: atmossync.CommitVersion, Data: postRoot, Rev: string(rev)}
	require.NoError(t, c.Sign(key))
	commitBytes, err := c.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitBytes)
	require.NoError(t, r.Store.PutBlock(commitCID, commitBytes))
	memStore, ok := r.Store.(*mst.MemBlockStore)
	require.True(t, ok)
	var carBuf bytes.Buffer
	cw, err := car.NewWriter(&carBuf, []cbor.CID{commitCID})
	require.NoError(t, err)
	for cid, data := range memStore.All() {
		require.NoError(t, cw.WriteBlock(cid, data))
	}
	for i := range rows {
		rows[i].Rev = string(rev)
	}
	return &comatproto.SyncSubscribeRepos_Commit{
		Repo:     string(r.DID),
		Rev:      string(rev),
		Commit:   lextypes.LexCIDLink{Link: commitCID.String()},
		Blocks:   carBuf.Bytes(),
		PrevData: gt.Some(lextypes.LexCIDLink{Link: prevData.String()}),
		Ops:      ops,
	}, rows
}

// holdHotBatches holds every hot batch commit until release returns true, so
// the batches freeze first and their commits pipeline behind them.
type holdHotBatches struct {
	release func() bool
}

func (h *holdHotBatches) Yield(ctx context.Context, point string) error {
	if point != "begin/"+string(catalog.TxHotBatch) {
		return nil
	}
	for !h.release() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// sameRow compares the fields the consumer derives from upstream: seq and
// witnessed time are local.
func sameRow(a, b segment.Event) bool {
	return a.Kind == b.Kind && a.DID == b.DID && a.Collection == b.Collection &&
		a.Rkey == b.Rkey && a.Rev == b.Rev && bytes.Equal(a.Payload, b.Payload)
}

// TestConsumer_Hot_SplitCommitRelayCursor is the design §10.4 test: one
// upstream commit whose rows land in three hot batches, with the leader
// session ending between two of their commits. relay/cursor must never cover
// the commit before all its rows commit (storagefake.RelayWatch checks every
// commit), so the next session re-requests it, and nothing is lost. Delivery
// is at least once: the rows of the split commit that did commit are
// archived again on replay, and nothing else is.
func TestConsumer_Hot_SplitCommitRelayCursor(t *testing.T) {
	t.Parallel()
	for _, kind := range []storagefake.FaultKind{storagefake.FaultCommitFails, storagefake.FaultCommitLost} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			testSplitCommitRelayCursor(t, kind)
		})
	}
}

func testSplitCommitRelayCursor(t *testing.T, kind storagefake.FaultKind) {
	did := atmos.DID("did:plc:splitcommit")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: mstore, Tree: mst.NewTree(mstore)}
	emptyRoot, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)

	identityRow := func(seq int64) ([]byte, segment.Event) {
		id := &comatproto.SyncSubscribeRepos_Identity{DID: string(did), Handle: gt.Some("h.test"), Seq: seq, Time: "2026-09-25T00:00:00Z"}
		body, err := id.MarshalCBOR()
		require.NoError(t, err)
		return encodeFrame(t, "#identity", body), segment.Event{Kind: segment.KindIdentity, DID: string(did), Payload: body}
	}
	id1Frame, id1Row := identityRow(1)
	commit, commitRows := buildMultiOpCommit(t, repo, key, emptyRoot, "app.bsky.feed.post", "a", "b", "c")
	commit.Seq = 2
	commitBody, err := commit.MarshalCBOR()
	require.NoError(t, err)
	id3Frame, id3Row := identityRow(3)

	// Upstream seq -> the rows it archives.
	upstream := map[int64][]segment.Event{1: {id1Row}, 2: commitRows, 3: {id3Row}}
	watch := storagefake.NewRelayWatch(nil)
	for u := int64(1); u <= 3; u++ {
		watch.Expect(u, upstream[u]...)
	}

	f := &cursorFirehose{
		t:      t,
		seqs:   []int64{1, 2, 3},
		frames: [][]byte{id1Frame, encodeFrame(t, "#commit", commitBody), id3Frame},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	var (
		hold     atomic.Bool
		consumer atomic.Pointer[Consumer]
	)
	hold.Store(true)
	db := storagefake.New(storagefake.Config{
		RelayWatch: watch,
		OnViolation: func(rev uint64, err error) {
			t.Errorf("catalog revision %d: %v", rev, err)
		},
		// Hold session 1's commits until the consumer has finished the
		// split commit: batches frozen during it must still carry the
		// cursor from before it, even though they commit afterward.
		Scheduler: &holdHotBatches{release: func() bool {
			c := consumer.Load()
			return !hold.Load() || (c != nil && c.cursorValue() >= 2)
		}},
	})
	// Batches: seq 1's row, then one per op of the split commit. The
	// fault ends the session at the split commit's second batch.
	fault := &storagefake.Fault{Kind: kind, TxKind: catalog.TxHotBatch, Ordinal: 3}
	db.InjectFaults(fault)

	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err = s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	blob := memblob.New()
	archive := db.Archive().ArchiveID
	up, err := protocol.NewUploader(protocol.UploaderConfig{Blob: blob, ArchiveID: archive, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)
	reader, err := protocol.NewReader(protocol.ReaderConfig{Rows: protocol.DBRows{DB: db}, Blob: blob, ArchiveID: archive})
	require.NoError(t, err)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	const blockEvents = 16
	newVerifier := func() *atmossync.Verifier {
		v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
			Directory:  &identity.Directory{Resolver: &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{did: buildDIDDoc(did, key.PublicKey())}}},
			StateStore: atmossync.NewMemStateStore(),
			Policy:     gt.Some(atmossync.PolicyError),
			SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: "http://example.invalid"}})),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = v.Close() })
		return v
	}
	open := func(hc *ingest.HotConfig) *Consumer {
		hc.Uploader = up
		hc.BatchMaxEvents = 1
		hc.BatchMaxAge = time.Hour
		hc.BlockMaxAge = time.Hour
		c, err := Open(Config{
			Store:             db.MetaStore(nil),
			SeqKey:            catalog.MainSeqKey,
			CursorKey:         catalog.RelayCursorKey,
			RelayURL:          srv.URL,
			Logger:            logger,
			Verifier:          newVerifier(),
			MaxEventsPerBlock: blockEvents,
			Hot:               hc,
		})
		require.NoError(t, err)
		consumer.Store(c)
		return c
	}
	run := func(c *Consumer) (context.CancelFunc, chan error) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		errc := make(chan error, 1)
		go func() { errc <- c.Run(ctx) }()
		return cancel, errc
	}
	committedCursor := func() int64 {
		snap, err := db.Snapshot()
		require.NoError(t, err)
		v := snap.Meta[catalog.RelayCursorKey]
		require.Len(t, v, 9)
		return int64(binary.LittleEndian.Uint64(v[1:]))
	}
	committedRows := func() []segment.Event {
		rtx, err := db.BeginRead(t.Context())
		require.NoError(t, err)
		defer func() { require.NoError(t, rtx.Close(t.Context())) }()
		snap, err := catalog.LoadSnapshotFrames(t.Context(), rtx, 1)
		require.NoError(t, err)
		require.Empty(t, snap.ActiveBlocks, "nothing folds in this test")
		var rows []segment.Event
		for _, h := range snap.HotBatches {
			require.True(t, h.Inline)
			evs, err := segment.DecodeBlockFrame(h.Frame)
			require.NoError(t, err)
			rows = append(rows, evs...)
		}
		return rows
	}

	// Session 1 ends at the fault.
	failed := make(chan error, 1)
	c1 := open(&ingest.HotConfig{Session: s, OnFailure: func(err error) { failed <- err }})
	cancel1, errc1 := run(c1)
	select {
	case err := <-failed:
		require.ErrorIs(t, err, catalog.ErrSessionEnded)
	case <-time.After(5 * time.Second):
		t.Fatal("session 1 never failed")
	}
	cancel1()
	<-errc1
	_ = c1.Close()
	require.True(t, fault.Fired())
	hold.Store(false)

	cursor1 := committedCursor()
	require.Less(t, cursor1, int64(2), "relay/cursor covers the split commit before all its rows committed")
	rows1 := committedRows()

	// Session 2: session start, then the consumer resumes the open block.
	require.NoError(t, lease.Release(t.Context()))
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	s = catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	m, err := maintainer.Open(t.Context(), maintainer.Config{
		Session: s, Uploader: up, Objects: reader, MaxEventsPerBlock: blockEvents, Logger: logger,
	})
	require.NoError(t, err)
	resume, err := m.Rebuild(t.Context(), maintainer.RebuildConfig{BlockMaxAge: time.Hour, RelayCursor: watch.Check})
	require.NoError(t, err)
	c2 := open(&ingest.HotConfig{Session: s, Sink: m, Resume: resume, OnFailure: func(err error) { t.Errorf("session 2: %v", err) }})
	cancel2, errc2 := run(c2)
	require.Eventually(t, func() bool { return c2.cursorValue() >= 3 }, 5*time.Second, time.Millisecond)
	cancel2()
	<-errc2
	require.NoError(t, c2.Close())
	require.NoError(t, m.Close())

	require.Equal(t, []int64{0, cursor1}, f.requested())
	require.Equal(t, int64(3), committedCursor())

	// Every row of session 1 that committed, then every row of the
	// upstream seqs after cursor1: nothing lost, and the only duplicates
	// are the split commit's rows that committed before the session ended.
	want := append([]segment.Event(nil), rows1...)
	for u := cursor1 + 1; u <= 3; u++ {
		want = append(want, upstream[u]...)
	}
	got := committedRows()
	require.Len(t, got, len(want))
	for i := range got {
		require.True(t, sameRow(want[i], got[i]), "row %d: got %+v, want %+v", i, got[i], want[i])
		if i > 0 {
			require.Greater(t, got[i].Seq, got[i-1].Seq, "seqs are never reused")
		}
	}
	require.Greater(t, len(got), len(rows1)+len(upstream[3]), "the split commit was redelivered")
	require.NoError(t, db.Violation())
}

// TestConsumer_Hot_ChainStateCommitsWithLastRow pins the verifier state
// half of design §10.4: the hot batch holding an upstream commit's last row
// commits that commit's chain state too. The session ends at the next
// batch, so the relay cursor still sits before the commit; its redelivery
// must meet the new chain state and be dropped as a replay. Promoting after
// Append returned would leave all three rows durable under the old chain
// state, and the replay would archive the whole commit a second time.
func TestConsumer_Hot_ChainStateCommitsWithLastRow(t *testing.T) {
	t.Parallel()
	did := atmos.DID("did:plc:chainwithrow")
	key, err := crypto.GenerateP256()
	require.NoError(t, err)
	mstore := mst.NewMemBlockStore()
	repo := &atmosrepo.Repo{DID: did, Clock: atmos.NewTIDClock(0), Store: mstore, Tree: mst.NewTree(mstore)}
	emptyRoot, err := repo.Tree.WriteBlocks(repo.Store)
	require.NoError(t, err)

	commit, commitRows := buildMultiOpCommit(t, repo, key, emptyRoot, "app.bsky.feed.post", "a", "b", "c")
	commit.Seq = 1
	commitBody, err := commit.MarshalCBOR()
	require.NoError(t, err)
	id := &comatproto.SyncSubscribeRepos_Identity{DID: string(did), Handle: gt.Some("h.test"), Seq: 2, Time: "2026-09-25T00:00:00Z"}
	idBody, err := id.MarshalCBOR()
	require.NoError(t, err)
	f := &cursorFirehose{
		t:      t,
		seqs:   []int64{1, 2},
		frames: [][]byte{encodeFrame(t, "#commit", commitBody), encodeFrame(t, "#identity", idBody)},
	}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	db := storagefake.New(storagefake.Config{})
	// Batches: one per op of the commit, then the identity row's.
	fault := &storagefake.Fault{Kind: storagefake.FaultCommitFails, TxKind: catalog.TxHotBatch, Ordinal: 4}
	db.InjectFaults(fault)
	lease := db.NewLease()
	require.NoError(t, lease.Acquire(t.Context(), time.Hour))
	s := catalog.NewSession(catalog.SessionConfig{DB: db, Epoch: lease.Epoch()})
	_, err = s.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	up, err := protocol.NewUploader(protocol.UploaderConfig{Blob: memblob.New(), ArchiveID: db.Archive().ArchiveID, GCDelay: time.Hour, OrphanAge: time.Hour})
	require.NoError(t, err)

	ms := db.MetaStore(nil)
	ss := syncstate.New(ms)
	v, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  &identity.Directory{Resolver: &stubResolver{docs: map[atmos.DID]*identity.DIDDocument{did: buildDIDDoc(did, key.PublicKey())}}},
		StateStore: ss,
		Policy:     gt.Some(atmossync.PolicyError),
		SyncClient: gt.Some(atmossync.NewClient(atmossync.Options{Client: &xrpc.Client{Host: "http://example.invalid"}})),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.Close() })

	failed := make(chan error, 1)
	c, err := Open(Config{
		Store:             ms,
		SeqKey:            catalog.MainSeqKey,
		CursorKey:         catalog.RelayCursorKey,
		RelayURL:          srv.URL,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:          v,
		SyncStateStore:    ss,
		MaxEventsPerBlock: 16,
		Hot: &ingest.HotConfig{
			Session:        s,
			Uploader:       up,
			BatchMaxEvents: 1,
			BatchMaxAge:    time.Hour,
			BlockMaxAge:    time.Hour,
			OnFailure:      func(err error) { failed <- err },
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	select {
	case err := <-failed:
		require.ErrorIs(t, err, catalog.ErrSessionEnded)
	case <-ctx.Done():
		t.Fatal("the session never failed")
	}
	cancel()
	<-errc
	_ = c.Close()
	require.True(t, fault.Fired())

	snap, err := db.Snapshot()
	require.NoError(t, err)
	require.Len(t, snap.HotBatches, len(commitRows), "every row of the commit committed")
	chain, err := syncstate.New(db.MetaStore(nil)).LoadChain(t.Context(), did)
	require.NoError(t, err)
	require.NotNil(t, chain, "the commit's rows are durable without its chain state")
	require.Equal(t, commit.Rev, chain.Rev)
}

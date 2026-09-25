package storagefake_test

import (
	"encoding/binary"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

func relayRow(rkey string) segment.Event {
	return segment.Event{
		Kind: segment.KindCreate, DID: "did:plc:relay", Collection: "app.bsky.feed.post",
		Rkey: rkey, Rev: "r1", Payload: []byte(rkey),
	}
}

func relayCursorOp(v uint64) metastore.Op {
	val := make([]byte, 9)
	val[0] = 1
	binary.LittleEndian.PutUint64(val[1:], v)
	return metastore.Op{Kind: metastore.OpSet, Key: []byte(catalog.RelayCursorKey), Value: val}
}

// commitRelayBatch commits rows as one inline hot batch starting at first,
// with meta staged alongside.
func commitRelayBatch(t *testing.T, s *catalog.Session, first uint64, meta []metastore.Op, rows ...segment.Event) error {
	t.Helper()
	b, err := segment.NewBlockBuilder(len(rows))
	require.NoError(t, err)
	for i, ev := range rows {
		ev.Seq = first + uint64(i)
		_, err := b.Append(ev)
		require.NoError(t, err)
	}
	frame, _ := b.Encode()
	_, err = s.CommitHotBatch(t.Context(), catalog.HotBatch{
		FirstSeq: first, LastSeq: first + uint64(len(rows)) - 1, Frame: frame, Meta: meta,
	})
	return err
}

// Upstream seq 2 is one commit with two rows split across batches: the
// cursor may name it only once both are committed.
func TestRelayWatch(t *testing.T) {
	t.Parallel()
	a, b, c := relayRow("a"), relayRow("b"), relayRow("c")
	setup := func(t *testing.T) (*storagefake.DB, *catalog.Session) {
		t.Helper()
		w := storagefake.NewRelayWatch(nil)
		w.Expect(1, a)
		w.Expect(2, b, c)
		db := storagefake.New(storagefake.Config{RelayWatch: w})
		s := leaderSession(t, db)
		_, err := s.InitNamespace(t.Context(), catalog.Main, nil)
		require.NoError(t, err)
		return db, s
	}

	t.Run("per batch cursor", func(t *testing.T) {
		t.Parallel()
		db, s := setup(t)
		require.NoError(t, commitRelayBatch(t, s, 1, []metastore.Op{relayCursorOp(1)}, a))
		require.NoError(t, commitRelayBatch(t, s, 2, []metastore.Op{relayCursorOp(1)}, b))
		require.NoError(t, commitRelayBatch(t, s, 3, []metastore.Op{relayCursorOp(2)}, c))
		require.NoError(t, db.Violation())
	})
	t.Run("cursor covers an uncommitted row", func(t *testing.T) {
		t.Parallel()
		db, s := setup(t)
		require.NoError(t, commitRelayBatch(t, s, 1, []metastore.Op{relayCursorOp(2)}, a, b))
		require.ErrorContains(t, db.Violation(), "covers upstream seq 2")
	})
	t.Run("malformed cursor", func(t *testing.T) {
		t.Parallel()
		db, s := setup(t)
		bad := metastore.Op{Kind: metastore.OpSet, Key: []byte(catalog.RelayCursorKey), Value: []byte{2}}
		require.NoError(t, commitRelayBatch(t, s, 1, []metastore.Op{bad}, a))
		require.ErrorContains(t, db.Violation(), "not a v1 cursor")
	})
}

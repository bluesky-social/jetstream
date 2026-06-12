package world

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/api/lextypes"
	"github.com/stretchr/testify/require"
)

func TestEncodeCommitFrame_DecodableHeader(t *testing.T) {
	t.Parallel()
	cm := &comatproto.SyncSubscribeRepos_Commit{
		Repo:   "did:plc:abcdefghijklmnopqrstuvwx",
		Rev:    "3kabc123def4g",
		Seq:    1,
		Time:   "2024-01-01T00:00:00Z",
		Commit: lextypes.LexCIDLink{Link: "bafyreib3qkrfz72tqigv27faqg2s7pjlkvwqcmbqnggzvqtdmhdw5rgnxq"},
		Ops:    []comatproto.SyncSubscribeRepos_RepoOp{},
		Blobs:  []lextypes.LexCIDLink{},
		Blocks: []byte{},
	}
	frame, err := encodeCommitFrame(cm)
	require.NoError(t, err)

	// Sanity: header must start with a CBOR map header for {"op":1, "t":"#commit"}.
	require.True(t, bytes.HasPrefix(frame, []byte{0xa2}), "expected map(2) header")
}

func TestEncodeSyncFrame_DecodableHeader(t *testing.T) {
	t.Parallel()
	sync := &comatproto.SyncSubscribeRepos_Sync{
		DID:    "did:plc:abcdefghijklmnopqrstuvwx",
		Rev:    "3kabc123def4g",
		Seq:    1,
		Time:   "2024-01-01T00:00:00Z",
		Blocks: []byte{0x01, 0x02},
	}
	frame, err := encodeSyncFrame(sync)
	require.NoError(t, err)

	require.True(t, bytes.HasPrefix(frame, []byte{0xa2}), "expected map(2) header")
	body, ok := bytes.CutPrefix(frame, frameHeaderSync)
	require.True(t, ok, "expected #sync header")
	var roundTrip comatproto.SyncSubscribeRepos_Sync
	require.NoError(t, roundTrip.UnmarshalCBOR(body))
	require.Equal(t, sync.DID, roundTrip.DID)
	require.Equal(t, sync.Rev, roundTrip.Rev)
	require.Equal(t, sync.Seq, roundTrip.Seq)
	require.Equal(t, sync.Blocks, roundTrip.Blocks)
}

func TestPersistAndRange(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.FirehoseHistory = 3
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	for i := int64(1); i <= 5; i++ {
		require.NoError(t, w.persistFirehoseFrame(i, []byte{byte(i)}))
	}
	// Cap at 3: only 3..5 remain.
	frames, err := w.firehoseRange(0, 100)
	require.NoError(t, err)
	require.Len(t, frames, 3)
	require.Equal(t, byte(3), frames[0][0])
	require.Equal(t, byte(5), frames[2][0])

	// Cursor=4 returns only 5.
	frames, err = w.firehoseRange(4, 100)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, byte(5), frames[0][0])
}

func TestPersistFirehoseFramePersistsSeqInSameCommitPoint(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	require.NoError(t, w.persistFirehoseFrame(7, []byte("frame-7")))

	seq, err := w.loadSeq()
	require.NoError(t, err)
	require.Equal(t, int64(7), seq)
}

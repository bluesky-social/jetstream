package world

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)

// frameHeaderCommit, frameHeaderIdentity, frameHeaderAccount, frameHeaderInfo
// are the precomputed CBOR encodings of the {"op":1,"t":"#..."} headers
// that prefix every wire frame. atmos's streaming.decodeFrame reads
// these as two concatenated CBOR values (header map + body).
var (
	frameHeaderCommit   = mustEncodeFrameHeader("#commit")
	frameHeaderIdentity = mustEncodeFrameHeader("#identity")
	frameHeaderAccount  = mustEncodeFrameHeader("#account")
	frameHeaderInfo     = mustEncodeFrameHeader("#info")
)

func mustEncodeFrameHeader(typ string) []byte {
	// Map(2): "op"->1, "t"->typ. We hand-encode to avoid pulling in a
	// full CBOR encoder for tiny static maps.
	out := []byte{0xa2}
	out = append(out, encodeText("op")...)
	out = append(out, 0x01)
	out = append(out, encodeText("t")...)
	out = append(out, encodeText(typ)...)
	return out
}

// encodeText returns CBOR-encoded text-string bytes. Strings up to
// length 23 use the inline-length form; longer strings use the
// 1-byte-length form (0x78). All header strings ("op", "t",
// "#commit", …) fit under 23 chars.
func encodeText(s string) []byte {
	if len(s) > 23 {
		return append([]byte{0x78, byte(len(s))}, []byte(s)...)
	}
	return append([]byte{0x60 | byte(len(s))}, []byte(s)...)
}

// encodeCommitFrame serializes header + body for a #commit event.
func encodeCommitFrame(c *comatproto.SyncSubscribeRepos_Commit) ([]byte, error) {
	body, err := c.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode commit frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderCommit)+len(body))
	out = append(out, frameHeaderCommit...)
	out = append(out, body...)
	return out, nil
}

// encodeIdentityFrame and encodeAccountFrame mirror encodeCommitFrame
// for #identity and #account.
func encodeIdentityFrame(e *comatproto.SyncSubscribeRepos_Identity) ([]byte, error) {
	body, err := e.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode identity frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderIdentity)+len(body))
	out = append(out, frameHeaderIdentity...)
	out = append(out, body...)
	return out, nil
}

func encodeAccountFrame(e *comatproto.SyncSubscribeRepos_Account) ([]byte, error) {
	body, err := e.MarshalCBOR()
	if err != nil {
		return nil, fmt.Errorf("world: encode account frame body: %w", err)
	}
	out := make([]byte, 0, len(frameHeaderAccount)+len(body))
	out = append(out, frameHeaderAccount...)
	out = append(out, body...)
	return out, nil
}

// encodeInfoFrame builds a #info frame with the given name + message.
// Used by the relay handler to send "OutdatedCursor" before falling
// back to live streaming.
func encodeInfoFrame(name, message string) []byte {
	info := &comatproto.SyncSubscribeRepos_Info{
		Name:    name,
		Message: optString(message),
	}
	body, err := info.MarshalCBOR()
	if err != nil {
		// Static input shape; an error here would surface a bug in
		// atmos's marshaller, not a runtime condition.
		panic(fmt.Sprintf("world: encode #info: %v", err))
	}
	out := make([]byte, 0, len(frameHeaderInfo)+len(body))
	out = append(out, frameHeaderInfo...)
	out = append(out, body...)
	return out
}

// optString wraps a possibly-empty string in atmos's gt.Option.
// The empty string maps to None so the field is omitted from CBOR
// rather than encoded as an empty string.
func optString(s string) gt.Option[string] {
	if s == "" {
		return gt.None[string]()
	}
	return gt.Some(s)
}

// persistFirehoseFrame writes one frame at sim/firehose/<seq> and
// trims the oldest entries beyond cfg.FirehoseHistory. Caller is
// responsible for serializing calls (single-writer invariant).
func (w *World) persistFirehoseFrame(seq int64, frame []byte) error {
	b := w.db.NewBatch()
	defer func() { _ = b.Close() }()

	if err := stageFirehoseFrame(b, seq, frame, w.cfg.FirehoseHistory); err != nil {
		return err
	}

	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("world: commit firehose: %w", err)
	}
	return nil
}

func stageFirehoseFrame(b *pebble.Batch, seq int64, frame []byte, history int) error {
	if err := b.Set(keyFirehose(seq), frame, nil); err != nil {
		return fmt.Errorf("world: stage firehose row: %w", err)
	}
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], uint64(seq))
	if err := b.Set(keyMetaSeq, seqBuf[:], nil); err != nil {
		return fmt.Errorf("world: stage firehose seq: %w", err)
	}

	if history > 0 && seq > int64(history) {
		oldest := seq - int64(history) + 1
		if err := b.DeleteRange(keyFirehose(0), keyFirehose(oldest), nil); err != nil {
			return fmt.Errorf("world: trim firehose: %w", err)
		}
	}
	return nil
}

// firehoseRange returns frames with seq > cursor, capped at limit.
// Frames are returned in seq order. Used by relay/subscribe to replay
// history before joining the live fanout.
func (w *World) firehoseRange(cursor int64, limit int) ([][]byte, error) {
	if limit <= 0 {
		return nil, nil
	}
	lo := keyFirehose(cursor + 1)
	hi := keyFirehoseUpper()
	iter, err := w.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, fmt.Errorf("world: firehose iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	out := make([][]byte, 0, limit)
	for iter.First(); iter.Valid() && len(out) < limit; iter.Next() {
		out = append(out, append([]byte(nil), iter.Value()...))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("world: firehose iter error: %w", err)
	}
	return out, nil
}

// keyFirehoseUpper is the exclusive upper bound for a firehose range
// scan: lexicographically greater than any keyFirehose(seq).
func keyFirehoseUpper() []byte {
	out := append([]byte(nil), []byte("sim/firehose/")...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], (1<<63)-1)
	return append(out, buf[:]...)
}

// Scaffolding: these encode functions are unused until later tasks.
var (
	_ = encodeIdentityFrame
	_ = encodeAccountFrame
)

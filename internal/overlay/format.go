package overlay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
	"github.com/klauspost/compress/zstd"
)

const (
	magic        = "jsto"
	formatVer    = uint16(1)
	reasonAcct   = uint8(1)
	reasonSync   = uint8(2)
	frameHdrSize = 4 + 2 + 2 + 8 + 8 + 8 // magic, ver, flags, W, M, body_len

	// maxDecodedOverlayBytes caps the decoder's decompressed size to
	// stop a hostile/corrupt frame from ballooning memory (zstd bomb),
	// mirroring segment/zstd.go. The overlay is bounded by the tombstone
	// cap (~32M entries); 1 GiB leaves generous headroom.
	maxDecodedOverlayBytes uint64 = 1 << 30
)

var (
	overlayEncoder *zstd.Encoder
	overlayDecoder *zstd.Decoder
)

func init() {
	var err error
	overlayEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		panic(fmt.Sprintf("overlay: zstd encoder init: %v", err))
	}
	overlayDecoder, err = zstd.NewReader(nil,
		zstd.WithDecoderMaxMemory(maxDecodedOverlayBytes),
	)
	if err != nil {
		panic(fmt.Sprintf("overlay: zstd decoder init: %v", err))
	}
}

func reasonCode(s string) uint8 {
	switch s {
	case "account":
		return reasonAcct
	case "sync":
		return reasonSync
	default:
		// The reason originates from Jetstream's own tombstone set
		// (tombstone.observeLocked sets only "account"/"sync"). An
		// unexpected value is invalid internal state, not external
		// input: crash loud rather than silently mislabel it on the
		// wire (AGENTS.md crash-over-corruption directive).
		panic(fmt.Sprintf("overlay: unexpected tombstone reason %q", s))
	}
}

func reasonString(c uint8) (string, error) {
	switch c {
	case reasonAcct:
		return "account", nil
	case reasonSync:
		return "sync", nil
	default:
		return "", fmt.Errorf("overlay: unknown reason code %d", c)
	}
}

// Encode serializes snap into the jsto v1 wire format: a fixed framing
// carrying W and M, then a single zstd frame holding dictionary tables
// and columnar, delta-varint tombstone sections. Pure and deterministic:
// the same snapshot always yields identical bytes.
//
// Precondition: every tombstone seq in snap must be strictly greater
// than w (as guaranteed by tombstone.SnapshotRange(w, ...)); a
// violation panics.
func Encode(snap tombstone.Snapshot, w, m uint64) []byte {
	body := encodeBody(snap, w)
	frame := overlayEncoder.EncodeAll(body, nil)

	out := make([]byte, frameHdrSize+len(frame))
	copy(out[0:4], magic)
	binary.LittleEndian.PutUint16(out[4:6], formatVer)
	binary.LittleEndian.PutUint16(out[6:8], 0) // flags reserved
	binary.LittleEndian.PutUint64(out[8:16], w)
	binary.LittleEndian.PutUint64(out[16:24], m)
	binary.LittleEndian.PutUint64(out[24:32], uint64(len(frame)))
	copy(out[frameHdrSize:], frame)
	return out
}

// encodeBody builds the uncompressed columnar body. seqBase is W (the
// first seq delta in every group/section is seq-W).
func encodeBody(snap tombstone.Snapshot, seqBase uint64) []byte {
	// Every seq must be strictly above seqBase (W): snapshots come from
	// tombstone.SnapshotRange(W, ...), which excludes seq <= W. A
	// violation is an internal bug (mismatched W), and the delta
	// encoding would silently underflow — crash loud instead.
	for k, seq := range snap.Records {
		if seq <= seqBase {
			panic(fmt.Sprintf("overlay: record seq %d <= watermark %d for %s/%s/%s", seq, seqBase, k.DID, k.Collection, k.Rkey))
		}
	}
	for did, ts := range snap.DIDs {
		if ts.Seq <= seqBase {
			panic(fmt.Sprintf("overlay: did-tombstone seq %d <= watermark %d for %s", ts.Seq, seqBase, did))
		}
	}

	type recEntry struct {
		coll string
		rkey string
		seq  uint64
	}
	byDID := make(map[string][]recEntry)
	didSet := make(map[string]struct{})
	collSet := make(map[string]struct{})
	for k, seq := range snap.Records {
		byDID[k.DID] = append(byDID[k.DID], recEntry{coll: k.Collection, rkey: k.Rkey, seq: seq})
		didSet[k.DID] = struct{}{}
		collSet[k.Collection] = struct{}{}
	}
	for did := range snap.DIDs {
		didSet[did] = struct{}{}
	}

	dids := sortedKeys(didSet)
	colls := sortedKeys(collSet)
	didID := indexMap(dids)
	collID := indexMap(colls)

	var buf []byte
	appendStringTable := func(items []string) {
		buf = appendUvarint(buf, uint64(len(items)))
		for _, s := range items {
			buf = appendUvarint(buf, uint64(len(s)))
			buf = append(buf, s...)
		}
	}
	appendStringTable(dids)
	appendStringTable(colls)

	groupDIDs := make([]string, 0, len(byDID))
	for did := range byDID {
		groupDIDs = append(groupDIDs, did)
	}
	sort.Slice(groupDIDs, func(i, j int) bool { return didID[groupDIDs[i]] < didID[groupDIDs[j]] })

	buf = appendUvarint(buf, uint64(len(groupDIDs)))
	for _, did := range groupDIDs {
		entries := byDID[did]
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].seq != entries[j].seq {
				return entries[i].seq < entries[j].seq
			}
			if entries[i].coll != entries[j].coll {
				return entries[i].coll < entries[j].coll
			}
			return entries[i].rkey < entries[j].rkey
		})
		buf = appendUvarint(buf, uint64(didID[did]))
		buf = appendUvarint(buf, uint64(len(entries)))
		prev := seqBase
		for _, e := range entries {
			buf = appendUvarint(buf, uint64(collID[e.coll]))
			buf = appendUvarint(buf, uint64(len(e.rkey)))
			buf = append(buf, e.rkey...)
			buf = appendUvarint(buf, e.seq-prev)
			prev = e.seq
		}
	}

	didTombDIDs := make([]string, 0, len(snap.DIDs))
	for did := range snap.DIDs {
		didTombDIDs = append(didTombDIDs, did)
	}
	sort.Slice(didTombDIDs, func(i, j int) bool { return didID[didTombDIDs[i]] < didID[didTombDIDs[j]] })

	buf = appendUvarint(buf, uint64(len(didTombDIDs)))
	prev := seqBase
	for _, did := range didTombDIDs {
		ts := snap.DIDs[did]
		buf = appendUvarint(buf, uint64(didID[did]))
		buf = appendUvarint(buf, ts.Seq-prev)
		buf = append(buf, reasonCode(ts.Reason))
		prev = ts.Seq
	}
	return buf
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func indexMap(items []string) map[string]int {
	m := make(map[string]int, len(items))
	for i, s := range items {
		m[s] = i
	}
	return m
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

// errMalformed is returned by the decoder for any structurally invalid
// blob. The decoder never panics on hostile input.
var errMalformed = errors.New("overlay: malformed blob")

// Decode parses a jsto v1 overlay blob back into its watermark, maxSeq,
// and tombstone snapshot. It bounds every length against the buffer and
// returns an error (never panics) on malformed input. This is the
// reference decoder the future client library mirrors (issue #10).
func Decode(blob []byte) (w, m uint64, snap tombstone.Snapshot, err error) {
	if len(blob) < frameHdrSize {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: short header", errMalformed)
	}
	if string(blob[0:4]) != magic {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: bad magic", errMalformed)
	}
	if ver := binary.LittleEndian.Uint16(blob[4:6]); ver != formatVer {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: unsupported version %d", errMalformed, ver)
	}
	w = binary.LittleEndian.Uint64(blob[8:16])
	m = binary.LittleEndian.Uint64(blob[16:24])
	bodyLen := binary.LittleEndian.Uint64(blob[24:32])
	if uint64(len(blob)-frameHdrSize) != bodyLen {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: body length mismatch", errMalformed)
	}
	body, derr := overlayDecoder.DecodeAll(blob[frameHdrSize:], nil)
	if derr != nil {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: zstd: %w", errMalformed, derr)
	}

	snap = tombstone.Snapshot{
		Records: map[tombstone.RecordKey]uint64{},
		DIDs:    map[string]tombstone.DIDTombstone{},
	}
	c := &cursor{buf: body}

	dids, err := c.stringTable()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	colls, err := c.stringTable()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}

	groupCount, err := c.uvarint()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	for range groupCount {
		didIdx, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		if didIdx >= uint64(len(dids)) {
			return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: did index out of range", errMalformed)
		}
		entryCount, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		prev := w
		for range entryCount {
			collIdx, err := c.uvarint()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			if collIdx >= uint64(len(colls)) {
				return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: coll index out of range", errMalformed)
			}
			rkey, err := c.lenBytes()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			delta, err := c.uvarint()
			if err != nil {
				return 0, 0, tombstone.Snapshot{}, err
			}
			seq := prev + delta
			prev = seq
			snap.Records[tombstone.RecordKey{DID: dids[didIdx], Collection: colls[collIdx], Rkey: string(rkey)}] = seq
		}
	}

	didTombCount, err := c.uvarint()
	if err != nil {
		return 0, 0, tombstone.Snapshot{}, err
	}
	prev := w
	for range didTombCount {
		didIdx, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		if didIdx >= uint64(len(dids)) {
			return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: did index out of range", errMalformed)
		}
		delta, err := c.uvarint()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		rc, err := c.byteVal()
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		reason, err := reasonString(rc)
		if err != nil {
			return 0, 0, tombstone.Snapshot{}, err
		}
		seq := prev + delta
		prev = seq
		snap.DIDs[dids[didIdx]] = tombstone.DIDTombstone{Seq: seq, Reason: reason}
	}

	if c.off != len(c.buf) {
		return 0, 0, tombstone.Snapshot{}, fmt.Errorf("%w: trailing bytes", errMalformed)
	}
	return w, m, snap, nil
}

type cursor struct {
	buf []byte
	off int
}

func (c *cursor) uvarint() (uint64, error) {
	v, n := binary.Uvarint(c.buf[c.off:])
	if n <= 0 {
		return 0, fmt.Errorf("%w: bad uvarint", errMalformed)
	}
	c.off += n
	return v, nil
}

func (c *cursor) byteVal() (uint8, error) {
	if c.off >= len(c.buf) {
		return 0, fmt.Errorf("%w: eof reading byte", errMalformed)
	}
	b := c.buf[c.off]
	c.off++
	return b, nil
}

func (c *cursor) lenBytes() ([]byte, error) {
	n, err := c.uvarint()
	if err != nil {
		return nil, err
	}
	if uint64(c.off)+n > uint64(len(c.buf)) {
		return nil, fmt.Errorf("%w: length exceeds buffer", errMalformed)
	}
	b := c.buf[c.off : c.off+int(n)]
	c.off += int(n)
	return b, nil
}

func (c *cursor) stringTable() ([]string, error) {
	count, err := c.uvarint()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	for range count {
		b, err := c.lenBytes()
		if err != nil {
			return nil, err
		}
		out = append(out, string(b))
	}
	return out, nil
}

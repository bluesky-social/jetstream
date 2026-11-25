package repair

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
)

const (
	genPrefix    = "repair/gen/"
	jobPrefix    = "repair/job/"
	cutoffPrefix = "repair/cutoff/"
	inboxPrefix  = "repair/inbox/"

	jobRecordVersion  uint8 = 1
	inboxCodecVersion uint8 = 1
)

// Phase is the durable lifecycle phase of one active per-DID repair job.
type Phase string

const (
	// PhaseQueued means the per-DID gate is active and the replacement snapshot
	// has not yet been durably staged.
	PhaseQueued Phase = "queued"
	// PhasePublishing means the staged replacement artifact is durable and the
	// worker is streaming ClassBulk chunks starting at NextRow.
	PhasePublishing Phase = "publishing"
	// PhaseDraining means every replacement row is durable (or reset was
	// aborted before KindSync) and the worker is draining the DID's inbox.
	PhaseDraining Phase = "draining"
)

// Source identifies what triggered a repair job.
type Source string

const (
	SourceAsyncResync Source = "async_resync"
	SourceSyncEvent   Source = "sync_event"
	SourceFailedRetry Source = "failed_retry"
)

// JobRecord is the durable state of one active per-DID repair job stored under
// repair/job/<did>. Deleted once replacement output and inbox drain complete.
type JobRecord struct {
	Version       uint8     `json:"v"`
	DID           atmos.DID `json:"did"`
	JobID         string    `json:"job_id"`
	Generation    uint64    `json:"gen"`
	Source        Source    `json:"source"`
	Phase         Phase     `json:"phase"`
	UpstreamSeq   int64     `json:"upstream_seq,omitempty"`
	AppliedRev    string    `json:"applied_rev,omitempty"`
	TargetRev     string    `json:"target_rev,omitempty"`
	SnapshotRev   string    `json:"snapshot_rev,omitempty"`
	SnapshotChain []byte    `json:"snapshot_chain,omitempty"`
	StageKey      string    `json:"stage_key,omitempty"`
	StageSHA256   [32]byte  `json:"stage_sha256,omitempty"`
	TotalRows     int64     `json:"total_rows,omitempty"`
	NextRow       int64     `json:"next_row,omitempty"`
	DrainedPos    uint64    `json:"drained_pos,omitempty"`
	FollowUpRev   string    `json:"follow_up_rev,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// InboxGroup is one validated live upstream event group captured behind a
// gated DID's durable inbox.
type InboxGroup struct {
	Pos          uint64
	UpstreamSeq  int64
	Rev          string
	ChainState   []byte
	HostingState []byte
	Events       []segment.Event
}

func genKey(did atmos.DID) []byte {
	return []byte(genPrefix + string(did))
}

func jobKey(did atmos.DID) []byte {
	return []byte(jobPrefix + string(did))
}

func cutoffKey(did atmos.DID) []byte {
	return []byte(cutoffPrefix + string(did))
}

func inboxDIDPrefix(did atmos.DID) []byte {
	return []byte(inboxPrefix + string(did) + "/")
}

func inboxDIDRangeEnd(did atmos.DID) []byte {
	// '/' is ASCII 0x2f; '0' is 0x30, so prefix ending in "0" is the strict
	// exclusive upper bound for "<inboxPrefix><did>/...".
	return []byte(inboxPrefix + string(did) + "0")
}

func inboxKey(did atmos.DID, pos uint64) []byte {
	return fmt.Appendf(nil, "%s%s/%020d", inboxPrefix, string(did), pos)
}

func prefixUpperBound(prefix string) []byte {
	b := []byte(prefix)
	b[len(b)-1]++
	return b
}

func encodeGen(gen uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], gen)
	return b[:]
}

func decodeGen(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("repair: invalid generation length %d", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}

func encodeJob(j *JobRecord) ([]byte, error) {
	if j.Version == 0 {
		j.Version = jobRecordVersion
	}
	return json.Marshal(j)
}

func decodeJob(b []byte) (*JobRecord, error) {
	var j JobRecord
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("repair: decode job: %w", err)
	}
	if j.Version != jobRecordVersion {
		return nil, fmt.Errorf("repair: unsupported job version %d", j.Version)
	}
	if j.DID == "" {
		return nil, errors.New("repair: decoded job has empty DID")
	}
	return &j, nil
}

// encodeEvent writes one segment.Event in a compact deterministic binary form.
// Seq and WitnessedAt are omitted because the writer assigns both on append.
func encodeEvent(buf *bytes.Buffer, ev *segment.Event) {
	buf.WriteByte(byte(ev.Kind))
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(ev.UpstreamRelayCursor))
	buf.Write(tmp[:])
	writeBytes(buf, []byte(ev.DID))
	writeBytes(buf, []byte(ev.Collection))
	writeBytes(buf, []byte(ev.Rkey))
	writeBytes(buf, []byte(ev.Rev))
	writeBytes(buf, ev.Payload)
}

func decodeEvent(r *bytes.Reader, dst *segment.Event) error {
	kindByte, err := r.ReadByte()
	if err != nil {
		return err
	}
	var seqBuf [8]byte
	if _, err := io.ReadFull(r, seqBuf[:]); err != nil {
		return err
	}
	did, err := readBytes(r)
	if err != nil {
		return err
	}
	col, err := readBytes(r)
	if err != nil {
		return err
	}
	rkey, err := readBytes(r)
	if err != nil {
		return err
	}
	rev, err := readBytes(r)
	if err != nil {
		return err
	}
	payload, err := readBytes(r)
	if err != nil {
		return err
	}
	*dst = segment.Event{
		Kind:                segment.Kind(kindByte),
		UpstreamRelayCursor: int64(binary.BigEndian.Uint64(seqBuf[:])),
		DID:                 string(did),
		Collection:          string(col),
		Rkey:                string(rkey),
		Rev:                 string(rev),
		Payload:             payload,
	}
	return nil
}

func encodeInboxGroup(g *InboxGroup) []byte {
	var buf bytes.Buffer
	buf.WriteByte(inboxCodecVersion)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], g.Pos)
	buf.Write(tmp[:])
	binary.BigEndian.PutUint64(tmp[:], uint64(g.UpstreamSeq))
	buf.Write(tmp[:])
	writeBytes(&buf, []byte(g.Rev))
	writeBytes(&buf, g.ChainState)
	writeBytes(&buf, g.HostingState)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(g.Events)))
	buf.Write(cnt[:])
	for i := range g.Events {
		encodeEvent(&buf, &g.Events[i])
	}
	return buf.Bytes()
}

func decodeInboxGroup(b []byte) (*InboxGroup, error) {
	r := bytes.NewReader(b)
	ver, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("repair: decode inbox group version: %w", err)
	}
	if ver != inboxCodecVersion {
		return nil, fmt.Errorf("repair: unsupported inbox group version %d", ver)
	}
	var tmp [8]byte
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return nil, fmt.Errorf("repair: decode inbox pos: %w", err)
	}
	pos := binary.BigEndian.Uint64(tmp[:])
	if _, err := io.ReadFull(r, tmp[:]); err != nil {
		return nil, fmt.Errorf("repair: decode inbox upstream seq: %w", err)
	}
	upstreamSeq := int64(binary.BigEndian.Uint64(tmp[:]))
	revBytes, err := readBytes(r)
	if err != nil {
		return nil, fmt.Errorf("repair: decode inbox rev: %w", err)
	}
	chainState, err := readBytes(r)
	if err != nil {
		return nil, fmt.Errorf("repair: decode inbox chain state: %w", err)
	}
	hostingState, err := readBytes(r)
	if err != nil {
		return nil, fmt.Errorf("repair: decode inbox hosting state: %w", err)
	}
	var cntBuf [4]byte
	if _, err := io.ReadFull(r, cntBuf[:]); err != nil {
		return nil, fmt.Errorf("repair: decode inbox event count: %w", err)
	}
	cnt := int(binary.BigEndian.Uint32(cntBuf[:]))
	// Guard against malformed count claiming more events than remaining bytes.
	if cnt > r.Len() {
		return nil, fmt.Errorf("repair: inbox event count %d exceeds remaining %d bytes", cnt, r.Len())
	}
	events := make([]segment.Event, cnt)
	for i := range cnt {
		if err := decodeEvent(r, &events[i]); err != nil {
			return nil, fmt.Errorf("repair: decode inbox event %d: %w", i, err)
		}
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("repair: trailing %d bytes in inbox group", r.Len())
	}
	return &InboxGroup{
		Pos:          pos,
		UpstreamSeq:  upstreamSeq,
		Rev:          string(revBytes),
		ChainState:   chainState,
		HostingState: hostingState,
		Events:       events,
	}, nil
}

func writeBytes(buf *bytes.Buffer, b []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
	buf.Write(lenBuf[:])
	buf.Write(b)
}

func readBytes(r *bytes.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	if n == 0 {
		return nil, nil
	}
	if n > r.Len() {
		return nil, fmt.Errorf("length %d exceeds remaining %d bytes", n, r.Len())
	}
	out := make([]byte, n)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

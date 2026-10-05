package segment

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/jcalabro/gloom"
)

// RecordKey names one record path: the key of a record tombstone.
type RecordKey struct {
	DID        string
	Collection string
	Rkey       string
}

// Tombstones is the drop rule a sparse rewrite applies, compiled once
// per compaction chunk and shared, read-only, by every segment rewrite
// in it. A row is dropped when it materializes a record (Create,
// Update, CreateResync), its seq is at most the chunk bound, and a DID
// tombstone or a tombstone on its record path has a strictly greater
// seq. This is tombstone.Snapshot.ShouldDrop plus the compactor's
// chunk bound.
type Tombstones struct {
	dids    map[string]uint64
	records map[RecordKey]uint64
	// maxSeq bounds the rows a rewrite may drop.
	maxSeq uint64
	// topSeq is the highest tombstone seq: no block starting at or
	// above it can lose a row.
	topSeq uint64
	// probes indexes both maps by DID for candidate-block selection.
	probes map[string]*didProbe
}

// didProbe is everything that can drop rows of one DID: its DID
// tombstone seq, if any, and the highest record-tombstone seq per
// collection.
type didProbe struct {
	did         string
	didSeq      uint64
	collections map[string]uint64
}

// NewTombstones compiles a drop rule from DID tombstone seqs and record
// tombstone seqs. Rows above maxSeq are never dropped; zero means no
// bound. The maps are retained, not copied: the caller must not modify
// them while the Tombstones is in use.
func NewTombstones(dids map[string]uint64, records map[RecordKey]uint64, maxSeq uint64) *Tombstones {
	if maxSeq == 0 {
		maxSeq = math.MaxUint64
	}
	t := &Tombstones{
		dids:    dids,
		records: records,
		maxSeq:  maxSeq,
		probes:  make(map[string]*didProbe, len(dids)),
	}
	probe := func(did string) *didProbe {
		p, ok := t.probes[did]
		if !ok {
			p = &didProbe{did: did}
			t.probes[did] = p
		}
		return p
	}
	for did, seq := range dids {
		probe(did).didSeq = seq
		t.topSeq = max(t.topSeq, seq)
	}
	for key, seq := range records {
		p := probe(key.DID)
		if p.collections == nil {
			p.collections = make(map[string]uint64)
		}
		p.collections[key.Collection] = max(p.collections[key.Collection], seq)
		t.topSeq = max(t.topSeq, seq)
	}
	return t
}

// Len is the number of distinct DIDs with any tombstone.
func (t *Tombstones) Len() int { return len(t.probes) }

// drop reports whether ev is dropped, and whether a DID tombstone (not
// a record tombstone) drops it.
func (t *Tombstones) drop(ev *Event) (drop, didLevel bool) {
	if !ev.Kind.IsMaterialization() || ev.Seq > t.maxSeq {
		return false, false
	}
	if seq, ok := t.dids[ev.DID]; ok && seq > ev.Seq {
		return true, true
	}
	if seq, ok := t.records[RecordKey{DID: ev.DID, Collection: ev.Collection, Rkey: ev.Rkey}]; ok && seq > ev.Seq {
		return true, false
	}
	return false, false
}

// mayDrop reports whether p can drop a row of block b, whose collection
// names are colls. It is exact given the block's DID bloom has already
// hit p.did: every row p drops has a seq in [b.MinSeq, tombstone seq)
// and, for a record tombstone, the tombstone's collection.
func (p *didProbe) mayDrop(b BlockInfo, maxSeq uint64, colls []string) bool {
	if b.MinSeq > maxSeq {
		return false
	}
	if p.didSeq > b.MinSeq {
		return true
	}
	// Rows with no collection are in no block's collection set.
	if seq, ok := p.collections[""]; ok && seq > b.MinSeq {
		return true
	}
	for _, c := range colls {
		if seq, ok := p.collections[c]; ok && seq > b.MinSeq {
			return true
		}
	}
	return false
}

// DefaultSparseProbeLimit bounds the bloom probes a sparse rewrite makes
// before it stops narrowing and treats every block as a candidate.
// Probing costs tens of nanoseconds; past a few million probes it costs
// more than decoding the blocks it would skip.
const DefaultSparseProbeLimit = 1 << 22

// SparseOptions tunes SparseRewrite. The zero value is valid.
type SparseOptions struct {
	// Name identifies the segment in error messages. Optional.
	Name string
	// Reserve, when set, is called before each block decode with the
	// decode's approximate memory cost, and the returned release after
	// the block's rows are no longer held. A worker pool shares one
	// budget through it (design §12.6). An error aborts the rewrite.
	Reserve func(bytes int64) (release func(), err error)
	// OnDrop, when set, is called for every dropped row. didLevel is
	// true when a DID tombstone dropped it. ev aliases the decoded
	// block and is valid only during the call.
	OnDrop func(ev *Event, didLevel bool)
	// ProbeLimit overrides DefaultSparseProbeLimit. Negative disables
	// narrowing: every block that could hold a droppable row is a
	// candidate.
	ProbeLimit int
}

// SparseFrame is one re-encoded block of a sparse rewrite.
type SparseFrame struct {
	Block int
	// Frame is the block's zstd frame, without a length prefix.
	Frame []byte
}

// SparseResult is a sparse rewrite's output. When Rewritten is false
// no row was dropped and the source generation stands; only
// BlocksFetched is meaningful.
type SparseResult struct {
	Rewritten bool
	// Header and HeaderBytes are the new finalized header, and Footer
	// the new footer. Blocks not in Frames keep their source frames.
	Header      Header
	HeaderBytes []byte
	Footer      []byte
	// Frames holds the changed blocks in ascending block order, and
	// Reused every other block.
	Frames []SparseFrame
	Reused []int

	RowsDropped  uint64
	VanishedDIDs uint32
	// BlocksFetched counts every block fetched: candidates and the
	// vanished-DID check's extra reads.
	BlocksFetched int
	// Dense reports that the probe limit stopped narrowing, so every block
	// below the highest tombstone seq was a candidate.
	Dense bool
}

// SparseRewrite is the §12.2 segment rewrite over one sealed generation:
// its header and footer, with blocks read through fetch. It fetches only
// the blocks a tombstone can touch, plus the blocks the exact
// vanished-DID check needs, and does no other I/O.
//
// The output decodes to exactly the rows Rewrite would leave, and
// passes VerifySealedMetadata. It differs from Rewrite's in the DID
// blooms, which it copies unchanged (supersets, which the format
// allows), and in collection id order.
func SparseRewrite(header, footer []byte, fetch BlockFetcher, t *Tombstones, opts SparseOptions) (SparseResult, error) {
	if t == nil {
		return SparseResult{}, fmt.Errorf("%w: SparseRewrite tombstones are required", ErrInvalidConfig)
	}
	r, err := OpenReaderParts(header, footer, fetch, ReaderOptions{Name: opts.Name})
	if err != nil {
		return SparseResult{}, err
	}
	s := sparseRewrite{r: r, t: t, opts: opts, blocks: r.blocks, colls: r.parsedCollections}
	return s.run(footer)
}

type sparseRewrite struct {
	r      *Reader
	t      *Tombstones
	opts   SparseOptions
	blocks []BlockInfo
	colls  collectionIndex

	blooms  []*gloom.Filter
	decoded []bool
	// present holds every DID of a decoded block's surviving rows.
	present map[string]struct{}
	// lost holds every DID that lost a row.
	lost map[string]struct{}
	// dropped counts dropped rows per collection.
	dropped map[string]uint32
	// changed maps a changed block to its re-encoded frame and its
	// surviving rows' summary.
	changed map[int]*sparseBlock

	fetched     int
	rowsDropped uint64
	dense       bool
}

type sparseBlock struct {
	frame            []byte
	uncompressedSize int
	eventCount       uint32
	collections      map[string]struct{}
}

func (s *sparseRewrite) run(footer []byte) (SparseResult, error) {
	h := s.r.Header()
	if len(s.blocks) == 0 || len(s.t.probes) == 0 {
		return SparseResult{}, nil
	}
	candidates, err := s.candidates()
	if err != nil {
		return SparseResult{}, err
	}
	s.decoded = make([]bool, len(s.blocks))
	s.present = map[string]struct{}{}
	s.lost = map[string]struct{}{}
	s.dropped = map[string]uint32{}
	s.changed = map[int]*sparseBlock{}
	for _, i := range candidates {
		if err := s.dropBlock(i); err != nil {
			return SparseResult{}, err
		}
	}
	if s.rowsDropped == 0 {
		return SparseResult{BlocksFetched: s.fetched, Dense: s.dense}, nil
	}
	vanished, err := s.vanished()
	if err != nil {
		return SparseResult{}, err
	}
	if uint64(h.EventCount) < s.rowsDropped || h.UniqueDIDCount < vanished {
		return SparseResult{}, fmt.Errorf("%w: %s drops %d rows and %d DIDs, header has %d and %d",
			ErrCorruptSegment, s.opts.Name, s.rowsDropped, vanished, h.EventCount, h.UniqueDIDCount)
	}

	infos := make([]BlockInfo, len(s.blocks))
	frames := make([]SparseFrame, 0, len(s.changed))
	reused := make([]int, 0, len(s.blocks)-len(s.changed))
	off := uint64(ReservedHeaderBytes)
	for i, b := range s.blocks {
		info := b
		info.Offset = off
		if c, ok := s.changed[i]; ok {
			info.CompressedSize = uint32(len(c.frame))
			info.UncompressedSize = uint32(c.uncompressedSize)
			info.EventCount = c.eventCount
			frames = append(frames, SparseFrame{Block: i, Frame: c.frame})
		} else {
			reused = append(reused, i)
		}
		infos[i] = info
		off += 8 + uint64(info.CompressedSize)
	}

	collBytes, err := s.collectionIndex()
	if err != nil {
		return SparseResult{}, err
	}
	// The segment bloom and the per-block bloom region are copied as they
	// are: they sit between the block index, which keeps its length, and
	// the collection index.
	blooms := footer[h.DIDBloomOffset-h.FooterOffset : h.CollectionIndexOffset-h.FooterOffset]
	blockIndex := encodeBlockIndex(infos)
	newFooter := make([]byte, 0, len(blockIndex)+len(blooms)+len(collBytes))
	newFooter = append(newFooter, blockIndex...)
	newFooter = append(newFooter, blooms...)
	newFooter = append(newFooter, collBytes...)

	nh := h
	nh.EventCount -= uint32(s.rowsDropped)
	nh.UniqueDIDCount -= vanished
	nh.FooterOffset = off
	nh.BlockIndexOffset = off
	nh.DIDBloomOffset = off + uint64(len(blockIndex))
	nh.BlockDIDBloomOffset = nh.DIDBloomOffset + (h.BlockDIDBloomOffset - h.DIDBloomOffset)
	nh.CollectionIndexOffset = nh.BlockDIDBloomOffset + (h.CollectionIndexOffset - h.BlockDIDBloomOffset)
	nh.Checksum = 0
	headerBytes := encodeHeader(nh)
	nh.Checksum = xxh3HeaderFooter(headerBytes, newFooter)
	binary.LittleEndian.PutUint64(headerBytes[4:12], nh.Checksum)

	return SparseResult{
		Rewritten:     true,
		Header:        nh,
		HeaderBytes:   headerBytes,
		Footer:        newFooter,
		Frames:        frames,
		Reused:        reused,
		RowsDropped:   s.rowsDropped,
		VanishedDIDs:  vanished,
		BlocksFetched: s.fetched,
		Dense:         s.dense,
	}, nil
}

// candidates returns the blocks a tombstone can touch (§12.2 step 1),
// ascending. Past the probe limit it stops narrowing and returns every
// block that holds rows below the highest tombstone seq.
func (s *sparseRewrite) candidates() ([]int, error) {
	limit := s.opts.ProbeLimit
	if limit == 0 {
		limit = DefaultSparseProbeLimit
	}
	var probes []*didProbe
	dense := limit < 0 || len(s.t.probes) > limit
	if !dense {
		for did, p := range s.t.probes {
			// Rows with no DID are in no bloom, so a probe for them
			// can never be ruled out by one.
			if did == "" || s.r.segmentBloom == nil || s.r.segmentBloom.TestString(did) {
				probes = append(probes, p)
			}
		}
		if len(probes) == 0 {
			return nil, nil
		}
		dense = len(probes)*len(s.blocks) > limit
	}
	if !dense {
		if _, err := s.loadBlooms(); err != nil {
			return nil, err
		}
	}

	s.dense = dense
	var out []int
	for i, b := range s.blocks {
		if b.EventCount == 0 || b.MinSeq > s.t.maxSeq {
			continue
		}
		if dense {
			if b.MinSeq < s.t.topSeq {
				out = append(out, i)
			}
			continue
		}
		colls, err := s.blockCollectionNames(i)
		if err != nil {
			return nil, err
		}
		for _, p := range probes {
			if p.did != "" && !s.blooms[i].TestString(p.did) {
				continue
			}
			if p.mayDrop(b, s.t.maxSeq, colls) {
				out = append(out, i)
				break
			}
		}
	}
	return out, nil
}

func (s *sparseRewrite) loadBlooms() ([]*gloom.Filter, error) {
	if s.blooms != nil {
		return s.blooms, nil
	}
	blooms, err := s.r.LoadAllBlockBlooms()
	if err != nil {
		return nil, err
	}
	s.blooms = blooms
	return blooms, nil
}

func (s *sparseRewrite) blockCollectionNames(i int) ([]string, error) {
	ids := s.colls.blockBitmasks[i]
	names := make([]string, len(ids))
	for j, id := range ids {
		if int(id) >= len(s.colls.stringTable) {
			return nil, fmt.Errorf("%w: %s block %d references collection id %d, table has %d",
				ErrInvalidFooter, s.opts.Name, i, id, len(s.colls.stringTable))
		}
		names[j] = s.colls.stringTable[id]
	}
	return names, nil
}

// eventBytes is one decoded Event's own size, beside the decompressed
// block its strings and payload alias.
const eventBytes = int64(unsafe.Sizeof(Event{}))

// decode fetches and decodes block i under the memory budget. The rows
// alias the decompressed block and are valid until release.
func (s *sparseRewrite) decode(i int) (events []Event, frame []byte, uncompressedSize int, release func(), err error) {
	b := s.blocks[i]
	release = func() {}
	if s.opts.Reserve != nil {
		release, err = s.opts.Reserve(int64(b.CompressedSize) + int64(b.UncompressedSize) + int64(b.EventCount)*eventBytes)
		if err != nil {
			return nil, nil, 0, nil, err
		}
	}
	s.fetched++
	s.decoded[i] = true
	frame, err = s.r.readFrame(i)
	if err == nil {
		events, uncompressedSize, err = decodeBlockCompressedSized(frame)
	}
	if err == nil && uint64(len(events)) != uint64(b.EventCount) {
		err = fmt.Errorf("%w: block holds %d rows, block index says %d", ErrCorruptSegment, len(events), b.EventCount)
	}
	if err != nil {
		release()
		return nil, nil, 0, nil, fmt.Errorf("segment: sparse rewrite %s block %d: %w", s.opts.Name, i, err)
	}
	return events, frame, uncompressedSize, release, nil
}

// dropBlock applies the drop rule to candidate block i (§12.2 steps 2
// and 4), re-encoding it when it loses a row.
func (s *sparseRewrite) dropBlock(i int) error {
	events, _, _, release, err := s.decode(i)
	if err != nil {
		return err
	}
	defer release()
	kept := events[:0]
	for j := range events {
		ev := &events[j]
		drop, didLevel := s.t.drop(ev)
		if !drop {
			kept = append(kept, *ev)
			continue
		}
		s.rowsDropped++
		if ev.DID != "" {
			if _, ok := s.lost[ev.DID]; !ok {
				s.lost[strings.Clone(ev.DID)] = struct{}{}
			}
		}
		if ev.Collection != "" {
			if _, ok := s.dropped[ev.Collection]; !ok {
				s.dropped[strings.Clone(ev.Collection)] = 0
			}
			s.dropped[ev.Collection]++
		}
		if s.opts.OnDrop != nil {
			s.opts.OnDrop(ev, didLevel)
		}
	}
	s.addPresent(kept)
	if len(kept) == len(events) {
		return nil
	}

	c := &sparseBlock{eventCount: uint32(len(kept)), collections: map[string]struct{}{}}
	if len(kept) == 0 {
		c.frame = encodeEmptyBlockCompressed()
		c.uncompressedSize = len(encodeEmptyBlock())
	} else {
		c.frame, c.uncompressedSize, err = encodeBlockCompressedSized(kept)
		if err != nil {
			return fmt.Errorf("segment: sparse rewrite %s encode block %d: %w", s.opts.Name, i, err)
		}
	}
	for j := range kept {
		ev := &kept[j]
		if ev.Collection != "" {
			if _, ok := c.collections[ev.Collection]; !ok {
				c.collections[strings.Clone(ev.Collection)] = struct{}{}
			}
		}
		if sentinel := didMarkerSentinel(ev.Kind); sentinel != "" {
			c.collections[sentinel] = struct{}{}
		}
	}
	s.changed[i] = c
	return nil
}

func (s *sparseRewrite) addPresent(events []Event) {
	for j := range events {
		if did := events[j].DID; did != "" {
			if _, ok := s.present[did]; !ok {
				s.present[strings.Clone(did)] = struct{}{}
			}
		}
	}
}

// vanished counts the DIDs that lost rows and have none left (§12.2
// step 3). It is exact: blooms only choose which undecoded blocks to
// fetch and check.
func (s *sparseRewrite) vanished() (uint32, error) {
	var n uint32
	for did := range s.lost {
		if _, ok := s.present[did]; ok {
			continue
		}
		if _, err := s.loadBlooms(); err != nil {
			return 0, err
		}
		for i, b := range s.blocks {
			if s.decoded[i] || b.EventCount == 0 || !s.blooms[i].TestString(did) {
				continue
			}
			events, _, _, release, err := s.decode(i)
			if err != nil {
				return 0, err
			}
			s.addPresent(events)
			release()
			if _, ok := s.present[did]; ok {
				break
			}
		}
		if _, ok := s.present[did]; !ok {
			n++
		}
	}
	return n, nil
}

// collectionIndex builds the new collection index (§12.2 step 5):
// counts reduced by the dropped rows, changed blocks' sets rebuilt from
// their surviving rows, and every collection no count or block still
// names removed, with the survivors keeping their relative order.
func (s *sparseRewrite) collectionIndex() ([]byte, error) {
	table := s.colls.stringTable
	idByName := make(map[string]uint32, len(table))
	for id, name := range table {
		idByName[name] = uint32(id)
	}
	counts := slices.Clone(s.colls.eventCounts)
	for name, n := range s.dropped {
		id, ok := idByName[name]
		if !ok {
			return nil, fmt.Errorf("%w: %s drops rows of collection %q, absent from the footer",
				ErrInvalidFooter, s.opts.Name, name)
		}
		if counts[id] < n {
			return nil, fmt.Errorf("%w: %s drops %d rows of collection %q, footer counts %d",
				ErrInvalidFooter, s.opts.Name, n, name, counts[id])
		}
		counts[id] -= n
	}

	bitmasks := make([][]uint32, len(s.blocks))
	referenced := make([]bool, len(table))
	for i := range s.blocks {
		ids := s.colls.blockBitmasks[i]
		if c, ok := s.changed[i]; ok {
			ids = make([]uint32, 0, len(c.collections))
			for name := range c.collections {
				id, ok := idByName[name]
				if !ok {
					return nil, fmt.Errorf("%w: %s block %d holds collection %q, absent from the footer",
						ErrInvalidFooter, s.opts.Name, i, name)
				}
				ids = append(ids, id)
			}
		}
		for _, id := range ids {
			referenced[id] = true
		}
		bitmasks[i] = ids
	}

	remap := make([]uint32, len(table))
	var idx collectionIndex
	for id, name := range table {
		if counts[id] == 0 && !referenced[id] {
			continue
		}
		remap[id] = uint32(len(idx.stringTable))
		idx.stringTable = append(idx.stringTable, name)
		idx.eventCounts = append(idx.eventCounts, counts[id])
	}
	for _, ids := range bitmasks {
		out := make([]uint32, len(ids))
		for j, id := range ids {
			out[j] = remap[id]
		}
		sortUint32(out)
		idx.blockBitmasks = append(idx.blockBitmasks, out)
	}
	return encodeCollectionIndex(idx)
}

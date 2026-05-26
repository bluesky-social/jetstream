package world

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
)

// repoState mirrors what we persist per-account: the previous commit
// CID + rev + MST root + record count. The "previous" framing matches
// how subscribeRepos #commit envelopes need `since` (= previous rev)
// and `prevData` (= previous MST root). The fresh state after commit
// becomes the *current* state, and turns into "previous" the next
// time this account commits.
type repoState struct {
	Rev         string
	DataCID     cbor.CID // MST root
	CommitCID   cbor.CID // signed commit block CID
	RecordCount int
}

// pebbleStore is a *pebble.DB-backed mst.BlockStore scoped to one
// account, so MST node loads come from that account's blocks
// keyspace.
type pebbleStore struct {
	db  *pebble.DB
	idx int
	// writes accumulates new blocks created by Tree.WriteBlocks; we
	// flush them to pebble in a batch alongside the commit.
	writes map[cbor.CID][]byte
}

func (s *pebbleStore) GetBlock(cid cbor.CID) ([]byte, error) {
	if data, ok := s.writes[cid]; ok {
		return data, nil
	}
	val, closer, err := s.db.Get(keyAccountBlock(s.idx, cid.Bytes()))
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return append([]byte(nil), val...), nil
}

func (s *pebbleStore) PutBlock(cid cbor.CID, data []byte) error {
	if s.writes == nil {
		s.writes = make(map[cbor.CID][]byte)
	}
	s.writes[cid] = append([]byte(nil), data...)
	return nil
}

// newEmptyRepo constructs an in-memory *repo.Repo for an account
// with no records yet. Used by bootstrap and by callers that want
// to add records before the first commit.
func newEmptyRepo(a account) (*repo.Repo, error) {
	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	return &repo.Repo{
		DID:   a.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}, nil
}

// loadRepo reconstructs an account's *repo.Repo from its persisted
// state: MST root from sim/account/<idx>/state, MST node + record
// blocks from sim/account/<idx>/blocks/*. Reads on demand.
func (w *World) loadRepo(a account) (*repo.Repo, error) {
	state, err := w.loadState(a.Index)
	if err != nil {
		return nil, err
	}
	store := &pebbleStore{db: w.db, idx: a.Index}
	if !state.DataCID.Defined() {
		// First commit lifecycle — empty MST.
		return newEmptyRepo(a)
	}
	tree := mst.LoadTree(store, state.DataCID)
	return &repo.Repo{
		DID:   a.DID,
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  tree,
	}, nil
}

// commitAndPersist writes the repo's MST blocks, signs a fresh commit,
// and persists every new block plus the updated state under one
// pebble batch. Returns the post-commit state. New record blocks must
// already be in rp.Store before this is called.
func (w *World) commitAndPersist(a account, rp *repo.Repo) (repoState, error) {
	commit, err := rp.Commit(a.priv)
	if err != nil {
		return repoState{}, fmt.Errorf("world: commit account %d: %w", a.Index, err)
	}
	commitData, err := commit.EncodeCBOR()
	if err != nil {
		return repoState{}, fmt.Errorf("world: encode commit: %w", err)
	}
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)

	// Walk the tree to count records and capture key→cid for the
	// MST index keyspace. This also forces the mst.Tree to populate
	// so anything not already cached fails loudly.
	count := 0
	keyCID := make(map[string]cbor.CID)
	if err := rp.Tree.Walk(func(key string, val cbor.CID) error {
		count++
		keyCID[key] = val
		return nil
	}); err != nil {
		return repoState{}, fmt.Errorf("world: walk tree: %w", err)
	}

	state := repoState{
		Rev:         commit.Rev,
		DataCID:     commit.Data,
		CommitCID:   commitCID,
		RecordCount: count,
	}

	// Write everything as one batch.
	b := w.db.NewBatch()
	defer func() { _ = b.Close() }()

	// New blocks: every block the *pebbleStore captured during this
	// session, plus the commit block.
	if ds, ok := rp.Store.(*diffStore); ok {
		for cid, data := range ds.writes {
			if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
				return repoState{}, fmt.Errorf("world: stage block: %w", err)
			}
		}
		// Do NOT clear ds.writes here: the live-traffic caller needs them
		// to package a CAR diff after this returns.
	} else if ps, ok := rp.Store.(*pebbleStore); ok {
		for cid, data := range ps.writes {
			if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
				return repoState{}, fmt.Errorf("world: stage block: %w", err)
			}
		}
		ps.writes = nil
	} else if mem, ok := rp.Store.(*mst.MemBlockStore); ok {
		// Bootstrap path: empty repo started in-memory; flush all
		// blocks. Iterate via mst's All().
		for cid, data := range mem.All() {
			if err := b.Set(keyAccountBlock(a.Index, cid.Bytes()), data, nil); err != nil {
				return repoState{}, fmt.Errorf("world: stage block: %w", err)
			}
		}
	} else {
		return repoState{}, errors.New("world: unsupported BlockStore impl")
	}
	if err := b.Set(keyAccountBlock(a.Index, commitCID.Bytes()), commitData, nil); err != nil {
		return repoState{}, fmt.Errorf("world: stage commit block: %w", err)
	}

	// Refresh the MST key→cid index (clear-and-rewrite is fine; the
	// tree size is small per-account).
	prefix := fmt.Sprintf("sim/account/%010d/mst/", a.Index)
	if err := b.DeleteRange([]byte(prefix), []byte(prefix+"\xff"), nil); err != nil {
		return repoState{}, fmt.Errorf("world: clear mst index: %w", err)
	}
	for k, v := range keyCID {
		if err := b.Set(keyAccountMSTKey(a.Index, k), v.Bytes(), nil); err != nil {
			return repoState{}, fmt.Errorf("world: stage mst index: %w", err)
		}
	}

	if err := b.Set(keyAccountState(a.Index), encodeState(state), nil); err != nil {
		return repoState{}, fmt.Errorf("world: stage state: %w", err)
	}

	if err := b.Commit(pebble.NoSync); err != nil {
		return repoState{}, fmt.Errorf("world: commit batch: %w", err)
	}
	return state, nil
}

// loadState reads sim/account/<idx>/state. Missing rows return a zero
// state (= "no commit yet").
func (w *World) loadState(idx int) (repoState, error) {
	val, closer, err := w.db.Get(keyAccountState(idx))
	if errors.Is(err, pebble.ErrNotFound) {
		return repoState{}, nil
	}
	if err != nil {
		return repoState{}, fmt.Errorf("world: load state %d: %w", idx, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeState(val)
}

// encodeState/decodeState use a tiny hand-rolled format to dodge a
// CBOR struct tag dependency: rev_len (varint) | rev | dataCID_len |
// dataCID | commitCID_len | commitCID | recordCount (varint).
func encodeState(s repoState) []byte {
	dataBytes := s.DataCID.Bytes()
	commitBytes := s.CommitCID.Bytes()
	out := make([]byte, 0, 4+len(s.Rev)+4+len(dataBytes)+4+len(commitBytes)+4)
	out = appendUvarint(out, uint64(len(s.Rev)))
	out = append(out, s.Rev...)
	out = appendUvarint(out, uint64(len(dataBytes)))
	out = append(out, dataBytes...)
	out = appendUvarint(out, uint64(len(commitBytes)))
	out = append(out, commitBytes...)
	out = appendUvarint(out, uint64(s.RecordCount))
	return out
}

func decodeState(buf []byte) (repoState, error) {
	var s repoState
	revLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state rev len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < revLen {
		return s, errors.New("world: decode state: short buffer (rev)")
	}
	s.Rev = string(buf[:revLen])
	buf = buf[revLen:]

	dataLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state data len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < dataLen {
		return s, errors.New("world: decode state: short buffer (data)")
	}
	if dataLen > 0 {
		cid, err := cbor.ParseCIDBytes(buf[:dataLen])
		if err != nil {
			return s, fmt.Errorf("world: decode state data cid: %w", err)
		}
		s.DataCID = cid
	}
	buf = buf[dataLen:]

	commitLen, n, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state commit len: %w", err)
	}
	buf = buf[n:]
	if uint64(len(buf)) < commitLen {
		return s, errors.New("world: decode state: short buffer (commit)")
	}
	if commitLen > 0 {
		cid, err := cbor.ParseCIDBytes(buf[:commitLen])
		if err != nil {
			return s, fmt.Errorf("world: decode state commit cid: %w", err)
		}
		s.CommitCID = cid
	}
	buf = buf[commitLen:]

	count, _, err := readUvarint(buf)
	if err != nil {
		return s, fmt.Errorf("world: decode state count: %w", err)
	}
	s.RecordCount = int(count)
	return s, nil
}

func appendUvarint(b []byte, x uint64) []byte {
	for x >= 0x80 {
		b = append(b, byte(x)|0x80)
		x >>= 7
	}
	return append(b, byte(x))
}

func readUvarint(b []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, c := range b {
		if i >= 10 {
			return 0, 0, errors.New("uvarint too long")
		}
		if c < 0x80 {
			return x | uint64(c)<<s, i + 1, nil
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0, errors.New("uvarint truncated")
}

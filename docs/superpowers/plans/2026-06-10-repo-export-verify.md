# Repo Export Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `jetstream export-repo` and `jetstream verify-repo` so operators can export the repo state implied by local segments and compare its MST root against an authoritative PDS repo.

**Architecture:** Put reconstruction, CAR writing, and verification logic in `internal/repoexport`. Keep `cmd/jetstream` as a thin CLI layer that validates flags, calls the internal package, and renders operator-facing text. Exported CARs are rooted at the reconstructed MST data CID and contain MST node blocks plus record blocks.

**Tech Stack:** Go, urfave/cli v3, existing `segment` and `internal/ingest` readers, `github.com/jcalabro/atmos/mst`, `github.com/jcalabro/atmos/car`, `github.com/jcalabro/atmos/repo`, `github.com/jcalabro/atmos/sync`, `github.com/jcalabro/atmos/xrpc`, `github.com/jcalabro/jttp`, `github.com/stretchr/testify`.

---

## File Structure

- Create `internal/repoexport/reconstruct.go`: reconstruct local DID state from segment files into an MST-backed snapshot.
- Create `internal/repoexport/car.go`: write reconstructed snapshots as deterministic MST-root CAR files.
- Create `internal/repoexport/verify.go`: download authoritative CARs and compare authoritative commit data root to local reconstructed root.
- Create `internal/repoexport/reconstruct_test.go`: tests for create/update/delete replay and DID filtering.
- Create `internal/repoexport/car_test.go`: tests for exported CAR roots and block contents.
- Create `internal/repoexport/verify_test.go`: tests for match, mismatch, missing local DID, and malformed authoritative CAR behavior.
- Create `cmd/jetstream/repo_export.go`: CLI commands and text renderers for `export-repo` and `verify-repo`.
- Create `cmd/jetstream/repo_export_test.go`: CLI argument validation and exit-code rendering behavior.
- Modify `cmd/jetstream/main.go`: register `exportRepoCommand()` and `verifyRepoCommand()`.
- Modify `README.md`: add a short operator section for the two commands.

---

### Task 1: Reconstruct Local Repo Snapshot

**Files:**
- Create: `internal/repoexport/reconstruct.go`
- Create: `internal/repoexport/reconstruct_test.go`

- [ ] **Step 1: Write failing reconstruction tests**

Create `internal/repoexport/reconstruct_test.go`:

```go
package repoexport

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/stretchr/testify/require"
)

const (
	testDID   = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"
	otherDID  = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"
	testColl  = "app.bsky.feed.post"
	testRKey1 = "3mtestaaaaaaaa"
	testRKey2 = "3mtestbbbbbbbb"
)

func TestReconstruct_CreateOnlyBuildsExpectedRoot(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
		testEvent(segment.KindCreate, testDID, testColl, testRKey2, "3mrev2", []byte{0xa1, 0x61, 0x62, 0x61, 0x32}),
	})

	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)
	require.Equal(t, testDID, snap.DID)
	require.Equal(t, "3mrev2", snap.LatestRev)
	require.Equal(t, 2, snap.RecordCount)

	wantRoot := expectedRoot(t, map[string][]byte{
		testColl + "/" + testRKey1: []byte{0xa1, 0x61, 0x61, 0x61, 0x31},
		testColl + "/" + testRKey2: []byte{0xa1, 0x61, 0x62, 0x61, 0x32},
	})
	require.True(t, wantRoot.Equal(snap.Root), "root mismatch: want %s got %s", wantRoot, snap.Root)
}

func TestReconstruct_UpdateReplacesRecordCID(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
		testEvent(segment.KindUpdate, testDID, testColl, testRKey1, "3mrev2", []byte{0xa1, 0x61, 0x61, 0x61, 0x32}),
	})

	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)
	require.Equal(t, "3mrev2", snap.LatestRev)
	require.Equal(t, 1, snap.RecordCount)

	wantRoot := expectedRoot(t, map[string][]byte{
		testColl + "/" + testRKey1: []byte{0xa1, 0x61, 0x61, 0x61, 0x32},
	})
	require.True(t, wantRoot.Equal(snap.Root), "root mismatch: want %s got %s", wantRoot, snap.Root)
}

func TestReconstruct_DeleteRemovesRecord(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
		testEvent(segment.KindCreate, testDID, testColl, testRKey2, "3mrev2", []byte{0xa1, 0x61, 0x62, 0x61, 0x32}),
		testEvent(segment.KindDelete, testDID, testColl, testRKey1, "3mrev3", nil),
	})

	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)
	require.Equal(t, "3mrev3", snap.LatestRev)
	require.Equal(t, 1, snap.RecordCount)

	wantRoot := expectedRoot(t, map[string][]byte{
		testColl + "/" + testRKey2: []byte{0xa1, 0x61, 0x62, 0x61, 0x32},
	})
	require.True(t, wantRoot.Equal(snap.Root), "root mismatch: want %s got %s", wantRoot, snap.Root)
}

func TestReconstruct_IgnoresOtherDIDsAndNonCommitEvents(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, otherDID, testColl, testRKey1, "3mrev-other", []byte{0xa1, 0x61, 0x78, 0x61, 0x79}),
		{IndexedAt: 2, Kind: segment.KindIdentity, DID: testDID},
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
	})

	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)
	require.Equal(t, "3mrev1", snap.LatestRev)
	require.Equal(t, 1, snap.RecordCount)
}

func TestReconstruct_MissingDIDReturnsErrNoLocalRepo(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, otherDID, testColl, testRKey1, "3mrev-other", []byte{0xa1, 0x61, 0x78, 0x61, 0x79}),
	})

	_, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.ErrorIs(t, err, ErrNoLocalRepo)
}

func testEvent(kind segment.Kind, did, coll, rkey, rev string, payload []byte) segment.Event {
	return segment.Event{
		IndexedAt:  1,
		Kind:       kind,
		DID:        did,
		Collection: coll,
		Rkey:       rkey,
		Rev:        rev,
		Payload:    append([]byte(nil), payload...),
	}
}

func writeTestSegments(t *testing.T, events []segment.Event) string {
	t.Helper()
	dataDir := t.TempDir()
	segmentsDir := filepath.Join(dataDir, "segments")
	st, err := store.Open(filepath.Join(dataDir, "meta.pebble"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	w, err := ingest.Open(ingest.Config{
		SegmentsDir:       segmentsDir,
		Store:             st,
		MaxEventsPerBlock: 2,
		MaxSegmentBytes:   1 << 30,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	for i := range events {
		ev := events[i]
		require.NoError(t, w.Append(context.Background(), &ev))
	}
	require.NoError(t, w.SealActiveAndClose())
	return dataDir
}

func expectedRoot(t *testing.T, records map[string][]byte) cbor.CID {
	t.Helper()
	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	for path, payload := range records {
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, payload)
		require.NoError(t, store.PutBlock(cid, payload))
		require.NoError(t, tree.Insert(path, cid))
	}
	root, err := tree.WriteBlocks(store)
	require.NoError(t, err)
	return root
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```sh
just test ./internal/repoexport
```

Expected: FAIL because `internal/repoexport` and `Reconstruct` do not exist.

- [ ] **Step 3: Implement reconstruction**

Create `internal/repoexport/reconstruct.go`:

```go
package repoexport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bluesky-social/jetstream-v2/internal/ingest"
	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

var ErrNoLocalRepo = errors.New("repoexport: no local commit events for DID")

type Config struct {
	DataDir string
	DID     string
}

type Snapshot struct {
	DID         string
	LatestRev   string
	Root        cbor.CID
	RecordCount int
	records     map[string]cbor.CID
	store       *blockStore
}

type blockStore struct {
	blocks map[cbor.CID][]byte
}

func newBlockStore() *blockStore {
	return &blockStore{blocks: make(map[cbor.CID][]byte)}
}

func (s *blockStore) GetBlock(cid cbor.CID) ([]byte, error) {
	data, ok := s.blocks[cid]
	if !ok {
		return nil, fmt.Errorf("repoexport: block not found: %s", cid.String())
	}
	return data, nil
}

func (s *blockStore) PutBlock(cid cbor.CID, data []byte) error {
	s.blocks[cid] = append([]byte(nil), data...)
	return nil
}

func (s *blockStore) sortedBlocks() []carBlock {
	out := make([]carBlock, 0, len(s.blocks))
	for cid, data := range s.blocks {
		out = append(out, carBlock{CID: cid, Data: append([]byte(nil), data...)})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CID.String() < out[j].CID.String()
	})
	return out
}

type carBlock struct {
	CID  cbor.CID
	Data []byte
}

func Reconstruct(ctx context.Context, cfg Config) (Snapshot, error) {
	if cfg.DataDir == "" {
		return Snapshot{}, fmt.Errorf("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return Snapshot{}, fmt.Errorf("repoexport: DID is required")
	}

	st := newBlockStore()
	tree := mst.NewTree(st)
	records := make(map[string]cbor.CID)
	seen := false
	latestRev := ""

	roots := []string{
		filepath.Join(cfg.DataDir, "segments"),
		filepath.Join(cfg.DataDir, "backfill", "live_segments"),
	}
	for _, dir := range roots {
		if err := replaySegmentDir(ctx, dir, cfg.DID, tree, st, records, &seen, &latestRev); err != nil {
			return Snapshot{}, err
		}
	}
	if !seen {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNoLocalRepo, cfg.DID)
	}

	root, err := tree.WriteBlocks(st)
	if err != nil {
		return Snapshot{}, fmt.Errorf("repoexport: write MST blocks: %w", err)
	}
	return Snapshot{
		DID:         cfg.DID,
		LatestRev:   latestRev,
		Root:        root,
		RecordCount: len(records),
		records:     records,
		store:       st,
	}, nil
}

func replaySegmentDir(ctx context.Context, dir, did string, tree *mst.Tree, st *blockStore, records map[string]cbor.CID, seen *bool, latestRev *string) error {
	files, err := ingest.SegmentFiles(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := replaySegmentFile(ctx, file.Path, did, tree, st, records, seen, latestRev); err != nil {
			return err
		}
	}
	return nil
}

func replaySegmentFile(ctx context.Context, path, did string, tree *mst.Tree, st *blockStore, records map[string]cbor.CID, seen *bool, latestRev *string) error {
	rd, err := segment.Open(segment.ReaderConfig{Path: path})
	if err == nil {
		defer func() { _ = rd.Close() }()
		for i := range int(rd.Header().BlockCount) {
			events, err := rd.DecodeBlock(i)
			if err != nil {
				return fmt.Errorf("repoexport: decode sealed segment %s block %d: %w", path, i, err)
			}
			if err := replayEvents(ctx, events, did, tree, st, records, seen, latestRev); err != nil {
				return err
			}
		}
		return nil
	}
	if !errors.Is(err, segment.ErrActiveSegment) {
		return err
	}
	return segment.WalkActive(path, func(events []segment.Event) error {
		return replayEvents(ctx, events, did, tree, st, records, seen, latestRev)
	})
}

func replayEvents(ctx context.Context, events []segment.Event, did string, tree *mst.Tree, st *blockStore, records map[string]cbor.CID, seen *bool, latestRev *string) error {
	for i := range events {
		if err := ctx.Err(); err != nil {
			return err
		}
		ev := events[i]
		if ev.DID != did || !isCommitKind(ev.Kind) {
			continue
		}
		*seen = true
		*latestRev = ev.Rev
		key := ev.Collection + "/" + ev.Rkey
		switch ev.Kind {
		case segment.KindCreate, segment.KindUpdate:
			cid := cbor.ComputeCID(cbor.CodecDagCBOR, ev.Payload)
			if err := st.PutBlock(cid, ev.Payload); err != nil {
				return err
			}
			if err := tree.Insert(key, cid); err != nil {
				return fmt.Errorf("repoexport: insert %s: %w", key, err)
			}
			records[key] = cid
		case segment.KindDelete:
			if err := tree.Remove(key); err != nil {
				return fmt.Errorf("repoexport: remove %s: %w", key, err)
			}
			delete(records, key)
		}
	}
	return nil
}

func isCommitKind(kind segment.Kind) bool {
	return kind == segment.KindCreate || kind == segment.KindUpdate || kind == segment.KindDelete
}
```

- [ ] **Step 4: Run reconstruction tests**

Run:

```sh
just test ./internal/repoexport -run TestReconstruct
```

Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add internal/repoexport/reconstruct.go internal/repoexport/reconstruct_test.go
git commit -m "feat: reconstruct repo state from segments"
```

---

### Task 2: Write Reconstructed MST-Root CARs

**Files:**
- Create: `internal/repoexport/car.go`
- Create: `internal/repoexport/car_test.go`
- Modify: `internal/repoexport/reconstruct.go`

- [ ] **Step 1: Write failing CAR export tests**

Create `internal/repoexport/car_test.go`:

```go
package repoexport

import (
	"bytes"
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestWriteCAR_RootsAtReconstructedMSTRoot(t *testing.T) {
	t.Parallel()
	payload := []byte{0xa1, 0x61, 0x61, 0x61, 0x31}
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", payload),
	})
	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)

	var buf bytes.Buffer
	report, err := WriteCAR(&buf, snap)
	require.NoError(t, err)
	require.Equal(t, testDID, report.DID)
	require.Equal(t, snap.Root.String(), report.Root)
	require.Equal(t, snap.RecordCount, report.RecordCount)
	require.Greater(t, report.BytesWritten, int64(0))
	require.GreaterOrEqual(t, report.BlockCount, 2)

	header, blocks, err := car.ReadAll(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Len(t, header.Roots, 1)
	require.True(t, snap.Root.Equal(header.Roots[0]))

	recordCID := cbor.ComputeCID(cbor.CodecDagCBOR, payload)
	require.True(t, carHasBlock(blocks, recordCID, payload), "exported CAR missing record block %s", recordCID)
	require.True(t, carHasCID(blocks, snap.Root), "exported CAR missing MST root block %s", snap.Root)
}

func TestWriteCAR_DeterministicOutput(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey2, "3mrev1", []byte{0xa1, 0x61, 0x62, 0x61, 0x32}),
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev2", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
	})
	snap, err := Reconstruct(context.Background(), Config{DataDir: dataDir, DID: testDID})
	require.NoError(t, err)

	var a, b bytes.Buffer
	_, err = WriteCAR(&a, snap)
	require.NoError(t, err)
	_, err = WriteCAR(&b, snap)
	require.NoError(t, err)
	require.Equal(t, a.Bytes(), b.Bytes())
}

func carHasCID(blocks []car.Block, cid cbor.CID) bool {
	for _, b := range blocks {
		if b.CID.Equal(cid) {
			return true
		}
	}
	return false
}

func carHasBlock(blocks []car.Block, cid cbor.CID, data []byte) bool {
	for _, b := range blocks {
		if b.CID.Equal(cid) && bytes.Equal(b.Data, data) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```sh
just test ./internal/repoexport -run TestWriteCAR
```

Expected: FAIL because `WriteCAR` and `ExportReport` do not exist.

- [ ] **Step 3: Implement CAR writing**

Create `internal/repoexport/car.go`:

```go
package repoexport

import (
	"fmt"
	"io"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
)

type ExportReport struct {
	DID          string
	LatestRev    string
	Root         string
	RecordCount  int
	BlockCount   int
	BytesWritten int64
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

func WriteCAR(w io.Writer, snap Snapshot) (ExportReport, error) {
	if w == nil {
		return ExportReport{}, fmt.Errorf("repoexport: writer is required")
	}
	if snap.store == nil {
		return ExportReport{}, fmt.Errorf("repoexport: snapshot has no block store")
	}

	rawBlocks := snap.store.sortedBlocks()
	blocks := make([]car.Block, 0, len(rawBlocks))
	for _, b := range rawBlocks {
		blocks = append(blocks, car.Block{CID: b.CID, Data: b.Data})
	}

	cw := &countingWriter{w: w}
	if err := car.WriteAll(cw, []cbor.CID{snap.Root}, blocks); err != nil {
		return ExportReport{}, fmt.Errorf("repoexport: write CAR: %w", err)
	}
	return ExportReport{
		DID:          snap.DID,
		LatestRev:    snap.LatestRev,
		Root:         snap.Root.String(),
		RecordCount:  snap.RecordCount,
		BlockCount:   len(blocks),
		BytesWritten: cw.n,
	}, nil
}
```

- [ ] **Step 4: Run CAR tests**

Run:

```sh
just test ./internal/repoexport -run 'TestWriteCAR|TestReconstruct'
```

Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add internal/repoexport/car.go internal/repoexport/car_test.go internal/repoexport/reconstruct.go
git commit -m "feat: export reconstructed repo car"
```

---

### Task 3: Verify Local Root Against Authoritative Repo CAR

**Files:**
- Create: `internal/repoexport/verify.go`
- Create: `internal/repoexport/verify_test.go`

- [ ] **Step 1: Write failing verification tests**

Create `internal/repoexport/verify_test.go`:

```go
package repoexport

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bluesky-social/jetstream-v2/segment"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
	"github.com/jcalabro/atmos/repo"
	"github.com/stretchr/testify/require"
)

func TestVerify_MatchingRoots(t *testing.T) {
	t.Parallel()
	payload := []byte{0xa1, 0x61, 0x61, 0x61, 0x31}
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev1", payload),
	})
	authCAR := authoritativeCAR(t, testDID, "3mrev1", map[string][]byte{
		testColl + "/" + testRKey1: payload,
	})
	server := carServer(t, authCAR)

	report, err := Verify(context.Background(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: server.URL,
	})
	require.NoError(t, err)
	require.True(t, report.Match)
	require.Equal(t, testDID, report.DID)
	require.Equal(t, "3mrev1", report.AuthoritativeRev)
	require.Equal(t, report.AuthoritativeRoot, report.LocalRoot)
}

func TestVerify_MismatchingRootsReturnsReport(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev-local", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
	})
	authCAR := authoritativeCAR(t, testDID, "3mrev-auth", map[string][]byte{
		testColl + "/" + testRKey1: []byte{0xa1, 0x61, 0x61, 0x61, 0x32},
	})
	server := carServer(t, authCAR)

	report, err := Verify(context.Background(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: server.URL,
	})
	require.NoError(t, err)
	require.False(t, report.Match)
	require.NotEqual(t, report.AuthoritativeRoot, report.LocalRoot)
	require.Equal(t, "3mrev-auth", report.AuthoritativeRev)
	require.Equal(t, "3mrev-local", report.LocalLatestRev)
	require.Equal(t, 1, report.LocalRecordCount)
}

func TestVerify_MissingLocalRepoReturnsMismatchReport(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, otherDID, testColl, testRKey1, "3mrev-other", []byte{0xa1, 0x61, 0x78, 0x61, 0x79}),
	})
	authCAR := authoritativeCAR(t, testDID, "3mrev-auth", map[string][]byte{
		testColl + "/" + testRKey1: []byte{0xa1, 0x61, 0x61, 0x61, 0x31},
	})
	server := carServer(t, authCAR)

	report, err := Verify(context.Background(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: server.URL,
	})
	require.NoError(t, err)
	require.False(t, report.Match)
	require.Equal(t, "3mrev-auth", report.AuthoritativeRev)
	require.Empty(t, report.LocalRoot)
	require.Equal(t, 0, report.LocalRecordCount)
	require.Contains(t, report.Message, "no local commit events")
}

func TestVerify_MalformedAuthoritativeCARReturnsError(t *testing.T) {
	t.Parallel()
	dataDir := writeTestSegments(t, []segment.Event{
		testEvent(segment.KindCreate, testDID, testColl, testRKey1, "3mrev-local", []byte{0xa1, 0x61, 0x61, 0x61, 0x31}),
	})
	server := carServer(t, []byte("not a car"))

	_, err := Verify(context.Background(), VerifyConfig{
		DataDir:  dataDir,
		DID:      testDID,
		RelayURL: server.URL,
	})
	require.Error(t, err)
}

func carServer(t *testing.T, carData []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/xrpc/com.atproto.sync.getRepo", r.URL.Path)
		require.Equal(t, testDID, r.URL.Query().Get("did"))
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		_, _ = w.Write(carData)
	}))
}

func authoritativeCAR(t *testing.T, did, rev string, records map[string][]byte) []byte {
	t.Helper()
	store := mst.NewMemBlockStore()
	tree := mst.NewTree(store)
	for path, payload := range records {
		cid := cbor.ComputeCID(cbor.CodecDagCBOR, payload)
		require.NoError(t, store.PutBlock(cid, payload))
		require.NoError(t, tree.Insert(path, cid))
	}
	root, err := tree.WriteBlocks(store)
	require.NoError(t, err)
	commit := &repo.Commit{
		DID:     did,
		Version: 3,
		Data:    root,
		Rev:     rev,
		Sig:     []byte{1, 2, 3},
	}
	commitData, err := commit.EncodeCBOR()
	require.NoError(t, err)
	commitCID := cbor.ComputeCID(cbor.CodecDagCBOR, commitData)

	blocks := []car.Block{{CID: commitCID, Data: commitData}}
	for cid, data := range store.All() {
		blocks = append(blocks, car.Block{CID: cid, Data: data})
	}
	var buf bytes.Buffer
	require.NoError(t, car.WriteAll(&buf, []cbor.CID{commitCID}, blocks))
	_, loadedCommit, err := repo.LoadFromCAR(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, did, loadedCommit.DID)
	return buf.Bytes()
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```sh
just test ./internal/repoexport -run TestVerify
```

Expected: FAIL because `Verify`, `VerifyConfig`, and `VerifyReport` do not exist.

- [ ] **Step 3: Implement verification**

Create `internal/repoexport/verify.go`:

```go
package repoexport

import (
	"context"
	"errors"
	"fmt"
	"io"

	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/repo"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

type VerifyConfig struct {
	DataDir  string
	DID      string
	RelayURL string
}

type VerifyReport struct {
	DID               string
	Match             bool
	AuthoritativeRev  string
	AuthoritativeRoot string
	LocalLatestRev    string
	LocalRoot         string
	LocalRecordCount  int
	Message           string
}

func Verify(ctx context.Context, cfg VerifyConfig) (VerifyReport, error) {
	if cfg.DataDir == "" {
		return VerifyReport{}, fmt.Errorf("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return VerifyReport{}, fmt.Errorf("repoexport: DID is required")
	}
	if cfg.RelayURL == "" {
		cfg.RelayURL = "https://bsky.network"
	}

	authRev, authRoot, err := fetchAuthoritativeRoot(ctx, cfg.RelayURL, cfg.DID)
	if err != nil {
		return VerifyReport{}, err
	}
	report := VerifyReport{
		DID:               cfg.DID,
		AuthoritativeRev:  authRev,
		AuthoritativeRoot: authRoot,
	}

	snap, err := Reconstruct(ctx, Config{DataDir: cfg.DataDir, DID: cfg.DID})
	if err != nil {
		if errors.Is(err, ErrNoLocalRepo) {
			report.Message = err.Error()
			return report, nil
		}
		return VerifyReport{}, err
	}
	report.LocalLatestRev = snap.LatestRev
	report.LocalRoot = snap.Root.String()
	report.LocalRecordCount = snap.RecordCount
	report.Match = report.AuthoritativeRoot == report.LocalRoot
	if !report.Match {
		report.Message = "local reconstructed MST root does not match authoritative repo root"
	}
	return report, nil
}

func fetchAuthoritativeRoot(ctx context.Context, relayURL, did string) (rev string, root string, err error) {
	xc := &xrpc.Client{
		Host:       relayURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
		Retry:      gt.Some(xrpc.RetryPolicy{MaxAttempts: gt.Some(1)}),
	}
	sc := atmossync.NewClient(atmossync.Options{Client: xc})
	body, err := sc.GetRepoStream(ctx, did, "")
	if err != nil {
		return "", "", fmt.Errorf("repoexport: get authoritative repo: %w", err)
	}
	defer func() {
		if cerr := body.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	_, commit, err := repo.LoadFromCAR(body)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return "", "", fmt.Errorf("repoexport: decode authoritative CAR: %w", err)
		}
		return "", "", fmt.Errorf("repoexport: decode authoritative CAR: %w", err)
	}
	return commit.Rev, commit.Data.String(), nil
}
```

- [ ] **Step 4: Run verification tests**

Run:

```sh
just test ./internal/repoexport -run TestVerify
```

Expected: PASS.

- [ ] **Step 5: Run all repoexport tests**

Run:

```sh
just test ./internal/repoexport
```

Expected: PASS.

- [ ] **Step 6: Commit**

```sh
git add internal/repoexport/verify.go internal/repoexport/verify_test.go
git commit -m "feat: verify reconstructed repo roots"
```

---

### Task 4: Add CLI Commands

**Files:**
- Create: `cmd/jetstream/repo_export.go`
- Create: `cmd/jetstream/repo_export_test.go`
- Modify: `cmd/jetstream/main.go`

- [ ] **Step 1: Write failing CLI tests**

Create `cmd/jetstream/repo_export_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
	"github.com/stretchr/testify/require"
)

func TestExportRepoCommandRequiresDIDAndOutput(t *testing.T) {
	t.Parallel()
	app := newApp()
	var out bytes.Buffer
	app.Writer = &out

	err := app.Run(context.Background(), []string{"jetstream", "export-repo"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected exactly one DID argument")

	err = app.Run(context.Background(), []string{"jetstream", "export-repo", "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "--output is required")
}

func TestRenderVerifyReportExitSemantics(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	err := renderVerifyReport(&out, verifyReportForTest(true))
	require.NoError(t, err)
	require.Contains(t, out.String(), "repo verification: match")

	out.Reset()
	err = renderVerifyReport(&out, verifyReportForTest(false))
	require.ErrorIs(t, err, errRepoVerifyMismatch)
	require.Contains(t, out.String(), "repo verification: mismatch")
	require.Contains(t, out.String(), "authoritative_root:")
	require.Contains(t, out.String(), "local_root:")
}

func verifyReportForTest(match bool) repoexport.VerifyReport {
	return repoexport.VerifyReport{
		DID:               "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		Match:             match,
		AuthoritativeRev:  "3mrev-auth",
		AuthoritativeRoot: "bafy-authoritative",
		LocalLatestRev:    "3mrev-local",
		LocalRoot:         "bafy-local",
		LocalRecordCount:  12,
		Message:           "local reconstructed MST root does not match authoritative repo root",
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```sh
just test ./cmd/jetstream -run 'TestExportRepoCommandRequiresDIDAndOutput|TestRenderVerifyReportExitSemantics'
```

Expected: FAIL because `export-repo`, `renderVerifyReport`, and `errRepoVerifyMismatch` do not exist.

- [ ] **Step 3: Implement CLI command file**

Create `cmd/jetstream/repo_export.go`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
	"github.com/urfave/cli/v3"
)

var errRepoVerifyMismatch = errors.New("repo verification mismatch")

func exportRepoCommand() *cli.Command {
	return &cli.Command{
		Name:      "export-repo",
		Usage:     "Export one DID's local segment-derived repo state as an MST-root CAR",
		ArgsUsage: "<did>",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "data-dir", Usage: "Path to the Jetstream data directory", Sources: cli.EnvVars("JETSTREAM_DATA_DIR"), Value: "./data"},
			&cli.StringFlag{Name: "output", Usage: "Path to write the reconstructed CAR"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 1 {
				return fmt.Errorf("export-repo: expected exactly one DID argument, got %d", args.Len())
			}
			output := cmd.String("output")
			if output == "" {
				return fmt.Errorf("export-repo: --output is required")
			}
			snap, err := repoexport.Reconstruct(ctx, repoexport.Config{
				DataDir: cmd.String("data-dir"),
				DID:     args.First(),
			})
			if err != nil {
				return err
			}
			f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return fmt.Errorf("export-repo: open output: %w", err)
			}
			report, writeErr := repoexport.WriteCAR(f, snap)
			closeErr := f.Close()
			if writeErr != nil {
				return writeErr
			}
			if closeErr != nil {
				return fmt.Errorf("export-repo: close output: %w", closeErr)
			}
			return renderExportReport(cmd.Root().Writer, output, report)
		},
	}
}

func verifyRepoCommand() *cli.Command {
	return &cli.Command{
		Name:      "verify-repo",
		Usage:     "Compare one DID's local segment-derived MST root against the authoritative repo root",
		ArgsUsage: "<did>",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "data-dir", Usage: "Path to the Jetstream data directory", Sources: cli.EnvVars("JETSTREAM_DATA_DIR"), Value: "./data"},
			&cli.StringFlag{Name: "relay-url", Usage: "Base URL of the relay used for com.atproto.sync.getRepo", Sources: cli.EnvVars("JETSTREAM_RELAY_URL"), Value: "https://bsky.network"},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 1 {
				return fmt.Errorf("verify-repo: expected exactly one DID argument, got %d", args.Len())
			}
			report, err := repoexport.Verify(ctx, repoexport.VerifyConfig{
				DataDir:  cmd.String("data-dir"),
				DID:      args.First(),
				RelayURL: cmd.String("relay-url"),
			})
			if err != nil {
				return err
			}
			return renderVerifyReport(cmd.Root().Writer, report)
		},
	}
}

func renderExportReport(w io.Writer, output string, r repoexport.ExportReport) error {
	_, err := fmt.Fprintf(w,
		"repo export complete\n  did: %s\n  output: %s\n  latest_rev: %s\n  mst_root: %s\n  records: %d\n  blocks: %d\n  bytes: %d\n",
		r.DID, output, r.LatestRev, r.Root, r.RecordCount, r.BlockCount, r.BytesWritten,
	)
	return err
}

func renderVerifyReport(w io.Writer, r repoexport.VerifyReport) error {
	state := "mismatch"
	if r.Match {
		state = "match"
	}
	if _, err := fmt.Fprintf(w,
		"repo verification: %s\n  did: %s\n  authoritative_rev: %s\n  authoritative_root: %s\n  local_latest_rev: %s\n  local_root: %s\n  local_records: %d\n",
		state, r.DID, r.AuthoritativeRev, r.AuthoritativeRoot, r.LocalLatestRev, r.LocalRoot, r.LocalRecordCount,
	); err != nil {
		return err
	}
	if r.Message != "" {
		if _, err := fmt.Fprintf(w, "  message: %s\n", r.Message); err != nil {
			return err
		}
	}
	if !r.Match {
		return errRepoVerifyMismatch
	}
	return nil
}
```

- [ ] **Step 4: Register commands**

Modify `cmd/jetstream/main.go` in `newApp()` so the command list is:

```go
		Commands: []*cli.Command{
			serveCommand(),
			versionCommand(),
			inspectSegmentCommand(),
			inspectAllCommand(),
			exportRepoCommand(),
			verifyRepoCommand(),
		},
```

- [ ] **Step 5: Run CLI tests**

Run:

```sh
just test ./cmd/jetstream -run 'TestExportRepoCommandRequiresDIDAndOutput|TestRenderVerifyReportExitSemantics'
```

Expected: PASS.

- [ ] **Step 6: Run package tests**

Run:

```sh
just test ./cmd/jetstream ./internal/repoexport
```

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add cmd/jetstream/main.go cmd/jetstream/repo_export.go cmd/jetstream/repo_export_test.go
git commit -m "feat: add repo export and verify commands"
```

---

### Task 5: Documentation and Full Verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README**

Add this section after "Inspecting segment files" in `README.md`:

```md
## Exporting and verifying repos

Export one DID's repo state as reconstructed from local Jetstream segments:

    just run export-repo did:plc:4uz2445cjiw7w4nobfgnu35f --output=/tmp/repo.car

The exported CAR is rooted at the reconstructed MST data CID. It is data-equivalent to an atproto repo snapshot, but it is not rooted at the original signed commit block because Jetstream segments do not store that block.

Compare local segment-derived state against the authoritative repo served by the relay/PDS path:

    just run verify-repo did:plc:4uz2445cjiw7w4nobfgnu35f

`verify-repo` exits `0` when the MST roots match and exits `1` when they do not. Network failures, malformed authoritative CARs, and corrupt local segments are reported as command errors.
```

- [ ] **Step 2: Run focused tests**

Run:

```sh
just test ./internal/repoexport ./cmd/jetstream
```

Expected: PASS.

- [ ] **Step 3: Run full short test suite**

Run:

```sh
just test
```

Expected: PASS.

- [ ] **Step 4: Run lint**

Run:

```sh
just lint
```

Expected: PASS.

- [ ] **Step 5: Build binary**

Run:

```sh
just build
```

Expected: PASS and `bin/jetstream` exists.

- [ ] **Step 6: Commit docs**

```sh
git add README.md
git commit -m "docs: document repo export verification"
```

---

## Self-Review Notes

- Spec coverage: `export-repo` is implemented by Tasks 1, 2, and 4. `verify-repo` is implemented by Tasks 1, 3, and 4. Documentation and full verification are covered by Task 5.
- Segment format: no task modifies `segment` format code.
- Root comparison: `verify-repo` compares authoritative `commit.Data` to local `Snapshot.Root`.
- File checksum comparison: absent by design.
- Production network tests: absent from normal tests; verification tests use `httptest.Server`.

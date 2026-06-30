package segment

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeActiveWithFlushedBlocks creates an active (unsealed) segment file at
// path with `blocks` flushed blocks of `perBlock` events each, contiguous seqs
// starting at 1. Returns the writer (still open/active) and the highest seq.
func writeActiveWithFlushedBlocks(t *testing.T, path string, blocks, perBlock int) (*Writer, uint64) {
	t.Helper()
	w, err := New(Config{Path: path, MaxEventsPerBlock: perBlock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var seq uint64 = 1
	for range blocks {
		for range perBlock {
			full, err := w.Append(Event{
				Seq: seq, IndexedAt: int64(seq) * 1000, Kind: KindCreate,
				DID: "did:plc:walkseal", Collection: "app.bsky.feed.post",
				Rkey: "r", Rev: "v", Payload: []byte{0xa0},
			})
			if err != nil {
				t.Fatalf("Append seq=%d: %v", seq, err)
			}
			seq++
			// Flush exactly when the block fills.
			if full {
				if err := w.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
			}
		}
	}
	return w, seq - 1
}

// collectWalk runs WalkActive on path and returns the emitted seqs in order
// plus any error.
func collectWalk(path string) ([]uint64, error) {
	var got []uint64
	err := WalkActive(path, func(events []Event) error {
		for i := range events {
			got = append(got, events[i].Seq)
		}
		return nil
	})
	return got, err
}

// TestWalkActive_AfterSeal_StaticParse isolates Leak 2's core question:
// when WalkActive reads a file whose size now INCLUDES an appended footer
// (i.e. the file was sealed after the walker decided to read it), does the
// frame walk (a) read the real frames then stop cleanly, (b) fail loud, or
// (c) silently mis-decode footer bytes as events?
//
// Seal only appends the footer at the old EOF and patches the 256-byte header;
// it never rewrites the frame region [256, footerOffset). So the real frames
// are always intact. The risk is purely what walkActiveFrames does when it
// continues past footerOffset into the footer bytes.
func TestWalkActive_AfterSeal_StaticParse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg_active.jss")

	w, maxSeq := writeActiveWithFlushedBlocks(t, path, 3, 4) // 12 events, 3 blocks

	// Sanity: walking the active (pre-seal) file yields all 12 contiguous seqs.
	pre, err := collectWalk(path)
	if err != nil {
		t.Fatalf("pre-seal WalkActive: %v", err)
	}
	if len(pre) != int(maxSeq) {
		t.Fatalf("pre-seal: want %d events, got %d (%v)", maxSeq, len(pre), pre)
	}
	for i, s := range pre {
		if s != uint64(i+1) {
			t.Fatalf("pre-seal: non-contiguous at %d: %v", i, pre)
		}
	}

	preInfo, _ := os.Stat(path)

	// Seal the file: appends footer + patches header. File grows.
	if _, err := w.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	postInfo, _ := os.Stat(path)
	t.Logf("file size pre-seal=%d post-seal=%d (footer added %d bytes)",
		preInfo.Size(), postInfo.Size(), postInfo.Size()-preInfo.Size())

	// Now walk the SEALED file via the active walker (the exact thing that
	// happens when WalkActive stats the post-seal size and walks frames).
	post, postErr := collectWalk(path)

	t.Logf("post-seal WalkActive: err=%v, emitted %d seqs: %v", postErr, len(post), post)

	// Document the three possible outcomes explicitly.
	switch {
	case postErr != nil:
		t.Logf("OUTCOME: WalkActive FAILS LOUD after seal (acceptable: client disconnects+heals)")
		// Still assert it didn't emit phantom/duplicate events before failing.
		for i, s := range post {
			if s != uint64(i+1) {
				t.Fatalf("LOUD-but-CORRUPT: emitted non-contiguous seq before erroring at idx %d: %v", i, post)
			}
		}
	case len(post) == int(maxSeq):
		t.Logf("OUTCOME: WalkActive reads real frames then stops cleanly at the footer (best case)")
		for i, s := range post {
			if s != uint64(i+1) {
				t.Fatalf("clean-stop but non-contiguous at idx %d: %v", i, post)
			}
		}
	default:
		t.Fatalf("OUTCOME: SILENT CORRUPTION: WalkActive emitted %d seqs (want %d), no error; footer bytes likely mis-decoded as events: %v",
			len(post), maxSeq, post)
	}
}

// TestWalkActive_ConcurrentSeal hammers WalkActive against a file being sealed
// concurrently, to surface any window where the walk mis-parses or returns a
// torn/duplicated/holed event stream. The producer writes a fixed active file
// once; a sealer seals it at a random-ish moment while many walkers read.
func TestWalkActive_ConcurrentSeal(t *testing.T) {
	dir := t.TempDir()

	var (
		corrupt atomic.Bool
		message atomic.Pointer[string]
	)

	// Each iteration uses its own file so the seal happens-during-walk window
	// is fresh; we run many iterations to vary timing.
	const iterations = 200
	for it := 0; it < iterations && !corrupt.Load(); it++ {
		path := filepath.Join(dir, "seg.jss")
		_ = os.Remove(path)
		w, maxSeq := writeActiveWithFlushedBlocks(t, path, 4, 4) // 16 events

		var iwg sync.WaitGroup

		// Walkers read concurrently with the seal.
		const walkers = 8
		for range walkers {
			iwg.Go(func() {
				for k := 0; k < 50 && !corrupt.Load(); k++ {
					got, err := collectWalk(path)
					if err != nil {
						// Loud failure during a concurrent seal is acceptable
						// (the file is legitimately mid-mutation). What is NOT
						// acceptable is a no-error result that is wrong.
						continue
					}
					// No error => the emitted prefix MUST be a contiguous run
					// 1..n for some n <= maxSeq, with NO phantom seqs beyond
					// maxSeq and NO holes/dupes.
					for i, s := range got {
						if s != uint64(i+1) {
							msg := "non-contiguous/dup seq in no-error walk: " + sprintSeqs(got)
							message.Store(&msg)
							corrupt.Store(true)
							return
						}
					}
					if len(got) > int(maxSeq) {
						msg := "phantom events beyond maxSeq in no-error walk: " + sprintSeqs(got)
						message.Store(&msg)
						corrupt.Store(true)
						return
					}
				}
			})
		}

		// Sealer: seal at a slightly varied delay so the seal lands during a walk.
		iwg.Go(func() {
			time.Sleep(time.Duration(it%5) * time.Microsecond)
			_, _ = w.Seal()
		})

		iwg.Wait()
		_ = w.Close()
	}

	if corrupt.Load() {
		t.Fatalf("Leak 2 confirmed (silent mis-decode under concurrent seal): %s", *message.Load())
	}
	t.Logf("no silent corruption observed across %d iterations", iterations)
}

func sprintSeqs(s []uint64) string {
	return fmt.Sprintf("%v", s)
}

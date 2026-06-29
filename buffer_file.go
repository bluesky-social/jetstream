package jetstream

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jcalabro/gt"
)

const (
	// fileBufferFlushFrames and fileBufferFlushInterval bound how much
	// buffered live data a crash can lose: the buffer fsyncs after this many
	// frames or this much wall-clock time, whichever comes first. At-least-
	// once delivery covers the lost tail (the live cursor is only advanced
	// past durably-flushed frames), so a short window trades a tiny re-tail
	// for high append throughput.
	fileBufferFlushFrames   = 5000
	fileBufferFlushInterval = 5 * time.Second
)

// errBufferClosed is returned by Append/Replay/Truncate after Close, so misuse
// (reusing a closed buffer) surfaces as a deterministic error rather than a
// nil-pointer panic on the nil writer/file.
var errBufferClosed = fmt.Errorf("jetstream: live buffer is closed")

// fileLiveBuffer is a durable, append-only LiveBuffer backed by a single file.
//
// On-disk format is line-delimited and recovery-friendly: each record is
//
//	<seq> <verbatim-frame-json>\n
//
// The frame bytes are the single-line JSON exactly as received on
// /subscribe-v2 (json.Marshal emits no embedded newlines), so no re-encoding
// is needed and recovery is trivial: scan complete lines and discard a
// trailing partial line from a crash mid-append.
type fileLiveBuffer struct {
	mu        sync.Mutex
	f         *os.File
	w         *bufio.Writer
	path      string
	unflushed int
	lastFlush time.Time
	// now is injectable for deterministic flush-cadence tests.
	now func() time.Time
}

// NewFileLiveBuffer opens (creating if needed) a durable JSONL live buffer at
// path. An existing file is opened for append so a restart keeps prior frames.
// Callers should Close it to flush the final pending frames.
func NewFileLiveBuffer(path string) (LiveBuffer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("jetstream: open live buffer %q: %w", path, err)
	}
	return &fileLiveBuffer{
		f:         f,
		w:         bufio.NewWriter(f),
		path:      path,
		lastFlush: time.Now(),
		now:       time.Now,
	}, nil
}

func (b *fileLiveBuffer) Append(frames []LiveFrame) error {
	if len(frames) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil || b.w == nil {
		return errBufferClosed
	}
	for _, fr := range frames {
		if err := b.writeLine(fr); err != nil {
			return err
		}
		b.unflushed++
	}
	if b.unflushed >= fileBufferFlushFrames || b.now().Sub(b.lastFlush) >= fileBufferFlushInterval {
		return b.flushLocked()
	}
	return nil
}

// writeLine appends one "<seq> <frame>\n" record. A frame must not contain a
// newline (single-line JSON); reject one that does rather than corrupt the
// line framing.
func (b *fileLiveBuffer) writeLine(fr LiveFrame) error {
	if bytes.IndexByte(fr.Data, '\n') >= 0 {
		return fmt.Errorf("jetstream: live frame seq=%d contains a newline; cannot buffer", fr.Seq)
	}
	if _, err := b.w.WriteString(strconv.FormatUint(fr.Seq, 10)); err != nil {
		return fmt.Errorf("jetstream: write live buffer: %w", err)
	}
	if err := b.w.WriteByte(' '); err != nil {
		return fmt.Errorf("jetstream: write live buffer: %w", err)
	}
	if _, err := b.w.Write(fr.Data); err != nil {
		return fmt.Errorf("jetstream: write live buffer: %w", err)
	}
	if err := b.w.WriteByte('\n'); err != nil {
		return fmt.Errorf("jetstream: write live buffer: %w", err)
	}
	return nil
}

func (b *fileLiveBuffer) flushLocked() error {
	if err := b.w.Flush(); err != nil {
		return fmt.Errorf("jetstream: flush live buffer: %w", err)
	}
	if err := b.f.Sync(); err != nil {
		return fmt.Errorf("jetstream: fsync live buffer: %w", err)
	}
	b.unflushed = 0
	b.lastFlush = b.now()
	return nil
}

func (b *fileLiveBuffer) Replay(ctx context.Context, after gt.Option[uint64]) iter.Seq2[LiveFrame, error] {
	return func(yield func(LiveFrame, error) bool) {
		b.mu.Lock()
		if b.f == nil || b.w == nil {
			b.mu.Unlock()
			yield(LiveFrame{}, errBufferClosed)
			return
		}
		err := b.flushLocked()
		b.mu.Unlock()
		if err != nil {
			yield(LiveFrame{}, err)
			return
		}

		f, err := os.Open(b.path)
		if err != nil {
			yield(LiveFrame{}, fmt.Errorf("jetstream: reopen live buffer: %w", err))
			return
		}
		defer func() { _ = f.Close() }()

		err = forEachCompleteLine(f, func(line []byte) bool {
			if ctx.Err() != nil {
				yield(LiveFrame{}, ctx.Err())
				return false
			}
			fr, ok := parseLine(line)
			if !ok {
				return false // corrupt line: stop at the recovery boundary
			}
			// None replays everything (seqs start at 1); Some(n) skips Seq <= n.
			if after.HasVal() && fr.Seq <= after.Val() {
				return true
			}
			return yield(fr, nil)
		})
		if err != nil {
			yield(LiveFrame{}, fmt.Errorf("jetstream: read live buffer: %w", err))
		}
	}
}

// forEachCompleteLine calls fn for each newline-terminated line in r, stripping
// the trailing newline. A final line WITHOUT a trailing newline is a partial
// write from a crash mid-append: it is skipped (the recovery boundary), not
// passed to fn. Stops early if fn returns false.
func forEachCompleteLine(r io.Reader, fn func(line []byte) bool) error {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			if !fn(line[:len(line)-1]) {
				return nil
			}
		}
		// A non-nil err with no trailing newline means we hit EOF on a partial
		// line; drop it. io.EOF is the normal terminator.
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// parseLine splits "<seq> <frame>" into a LiveFrame. ok is false for a line
// missing the space separator or with an unparseable seq (a partial trailing
// write), which the reader treats as the recovery boundary.
func parseLine(line []byte) (LiveFrame, bool) {
	sp := -1
	for i, c := range line {
		if c == ' ' {
			sp = i
			break
		}
	}
	if sp <= 0 || sp == len(line)-1 {
		return LiveFrame{}, false
	}
	seq, err := strconv.ParseUint(string(line[:sp]), 10, 64)
	if err != nil {
		return LiveFrame{}, false
	}
	data := append([]byte(nil), line[sp+1:]...)
	return LiveFrame{Seq: seq, Data: data}, true
}

// Truncate rewrites the file keeping only frames with Seq > throughSeq. A
// rewrite (rather than in-place edit) keeps the append-only format simple and
// is bounded by the live backlog, not the whole archive.
func (b *fileLiveBuffer) Truncate(throughSeq uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil || b.w == nil {
		return errBufferClosed
	}
	if err := b.flushLocked(); err != nil {
		return err
	}

	src, err := os.Open(b.path)
	if err != nil {
		return fmt.Errorf("jetstream: open live buffer for truncate: %w", err)
	}
	srcClosed := false
	defer func() {
		if !srcClosed {
			_ = src.Close()
		}
	}()

	tmp := b.path + ".tmp"
	out, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("jetstream: create live buffer temp: %w", err)
	}
	tw := bufio.NewWriter(out)

	var writeErr error
	scanErr := forEachCompleteLine(src, func(line []byte) bool {
		fr, ok := parseLine(line)
		if !ok {
			return false // recovery boundary
		}
		if fr.Seq <= throughSeq {
			return true
		}
		if _, err := tw.Write(line); err != nil {
			writeErr = err
			return false
		}
		if err := tw.WriteByte('\n'); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		_ = out.Close()
		return fmt.Errorf("jetstream: write live buffer temp: %w", writeErr)
	}
	if scanErr != nil {
		_ = out.Close()
		return fmt.Errorf("jetstream: scan during truncate: %w", scanErr)
	}
	if err := tw.Flush(); err != nil {
		_ = out.Close()
		return fmt.Errorf("jetstream: flush live buffer temp: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return fmt.Errorf("jetstream: fsync live buffer temp: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("jetstream: close live buffer temp: %w", err)
	}

	// Close the read handle on the original before the swap: src has been fully
	// read into the temp by now, and an open handle on the destination makes the
	// rename fail on platforms (e.g. Windows) that reject replacing an open file.
	if err := src.Close(); err != nil {
		return fmt.Errorf("jetstream: close live buffer source during truncate: %w", err)
	}
	srcClosed = true

	// Swap the temp in and reopen the live writer at the new tail.
	if err := b.w.Flush(); err != nil {
		return fmt.Errorf("jetstream: flush before swap: %w", err)
	}
	if err := b.f.Close(); err != nil {
		return fmt.Errorf("jetstream: close live buffer before swap: %w", err)
	}
	if err := os.Rename(tmp, b.path); err != nil {
		// The swap failed but the original file is untouched at b.path. Reopen it
		// (discarding the temp) so the buffer stays usable rather than stranding
		// b.f/b.w on a closed descriptor and permanently breaking the buffer.
		_ = os.Remove(tmp)
		if rErr := b.reopenAppend(); rErr != nil {
			return errors.Join(
				fmt.Errorf("jetstream: rename live buffer temp: %w", err),
				fmt.Errorf("jetstream: reopen live buffer after failed rename: %w", rErr),
			)
		}
		return fmt.Errorf("jetstream: rename live buffer temp: %w", err)
	}
	if err := b.reopenAppend(); err != nil {
		return fmt.Errorf("jetstream: reopen live buffer after truncate: %w", err)
	}
	return nil
}

// reopenAppend reopens b.path for append and resets the buffered writer and
// flush bookkeeping. Caller holds b.mu. On error b.f/b.w are left nil so a
// subsequent call observes the closed state rather than a stale descriptor.
func (b *fileLiveBuffer) reopenAppend() error {
	nf, err := os.OpenFile(b.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		b.f = nil
		b.w = nil
		return err
	}
	b.f = nf
	b.w = bufio.NewWriter(nf)
	b.unflushed = 0
	b.lastFlush = b.now()
	return nil
}

func (b *fileLiveBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil {
		return nil
	}
	flushErr := b.flushLocked()
	closeErr := b.f.Close()
	b.f = nil
	b.w = nil
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

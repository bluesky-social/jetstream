package repair

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
)

var stageMagic = [4]byte{'J', 'S', 'R', 'S'}

const stageFormatVersion uint8 = 1

// StageMeta describes a finalized staged replacement artifact.
type StageMeta struct {
	Key       string
	SHA256    [32]byte
	TotalRows int64
	SizeBytes int64
}

// StageStore persists immutable staged replacement artifacts across crashes and
// leader failovers. Implementations: FSStageStore (local mode), BlobStageStore
// (disaggregated mode), and MemStageStore (tests / fallback).
type StageStore interface {
	Create(ctx context.Context, key string) (StageSink, error)
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
}

// StageSink receives a staged replacement artifact stream and makes it durable
// on Commit. Abort discards any partial write.
type StageSink interface {
	io.Writer
	Commit(ctx context.Context) error
	Abort() error
}

// StageWriter streams validated replacement rows into a StageSink without
// materializing a full []segment.Event slice in memory.
type StageWriter struct {
	key    string
	sink   StageSink
	bw     *bufio.Writer
	hasher hash.Hash
	rowBuf bytes.Buffer
	rows   int64
	size   int64
	closed bool
}

// NewStageWriter opens a new streaming staged replacement artifact under key.
func NewStageWriter(ctx context.Context, store StageStore, key string) (*StageWriter, error) {
	sink, err := store.Create(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("repair: create stage %s: %w", key, err)
	}
	h := sha256.New()
	// Hash before sink: pebble's in-memory vfs mutates the input buffer in
	// Write under -race (invariants.Enabled) to catch callers assuming p stays
	// unmodified. sha256 never mutates p.
	bw := bufio.NewWriterSize(io.MultiWriter(h, sink), 64<<10)
	var hdr [5]byte
	copy(hdr[:4], stageMagic[:])
	hdr[4] = stageFormatVersion
	n, err := bw.Write(hdr[:])
	if err != nil {
		_ = sink.Abort()
		return nil, fmt.Errorf("repair: write stage header %s: %w", key, err)
	}
	return &StageWriter{
		key:    key,
		sink:   sink,
		bw:     bw,
		hasher: h,
		size:   int64(n),
	}, nil
}

// WriteEvent encodes and appends one validated replacement event to the stage.
func (w *StageWriter) WriteEvent(ev *segment.Event) error {
	if w.closed {
		return errors.New("repair: stage writer already closed")
	}
	w.rowBuf.Reset()
	encodeEvent(&w.rowBuf, ev)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(w.rowBuf.Len()))
	n1, err := w.bw.Write(lenBuf[:])
	if err != nil {
		return fmt.Errorf("repair: write stage row length: %w", err)
	}
	n2, err := w.bw.Write(w.rowBuf.Bytes())
	if err != nil {
		return fmt.Errorf("repair: write stage row payload: %w", err)
	}
	w.rows++
	w.size += int64(n1 + n2)
	return nil
}

// Commit flushes and durably commits the staged artifact, returning its digest
// and row count.
func (w *StageWriter) Commit(ctx context.Context) (StageMeta, error) {
	if w.closed {
		return StageMeta{}, errors.New("repair: stage writer already closed")
	}
	w.closed = true
	if err := w.bw.Flush(); err != nil {
		_ = w.sink.Abort()
		return StageMeta{}, fmt.Errorf("repair: flush stage %s: %w", w.key, err)
	}
	if err := w.sink.Commit(ctx); err != nil {
		return StageMeta{}, fmt.Errorf("repair: commit stage %s: %w", w.key, err)
	}
	var sum [32]byte
	copy(sum[:], w.hasher.Sum(nil))
	return StageMeta{
		Key:       w.key,
		SHA256:    sum,
		TotalRows: w.rows,
		SizeBytes: w.size,
	}, nil
}

// Abort discards the unfinished staged artifact. Idempotent.
func (w *StageWriter) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.sink.Abort()
}

// StageReader streams replacement rows from a verified staged artifact,
// supporting O(1)-memory resume from an arbitrary row offset.
type StageReader struct {
	rc     io.ReadCloser
	br     *bufio.Reader
	rowBuf []byte
	rowIdx int64
}

// OpenStageReader opens key from store, verifies its SHA-256 digest against
// expected (when non-zero), validates the header, and skips skipRows records
// without decoding or retaining skipped payloads.
func OpenStageReader(ctx context.Context, store StageStore, key string, expected [32]byte, skipRows int64) (*StageReader, error) {
	// Verify integrity first before streaming any rows into the archive.
	if expected != ([32]byte{}) {
		rc, err := store.Open(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("repair: open stage %s for verify: %w", key, err)
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, rc)
		closeErr := rc.Close()
		if copyErr != nil {
			return nil, fmt.Errorf("repair: verify stage %s: %w", key, copyErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("repair: close stage %s after verify: %w", key, closeErr)
		}
		var got [32]byte
		copy(got[:], h.Sum(nil))
		if got != expected {
			return nil, fmt.Errorf("repair: stage %s checksum mismatch: got %x want %x", key, got, expected)
		}
	}

	rc, err := store.Open(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("repair: open stage %s: %w", key, err)
	}
	br := bufio.NewReaderSize(rc, 64<<10)
	var hdr [5]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("repair: read stage header %s: %w", key, err)
	}
	if !bytes.Equal(hdr[:4], stageMagic[:]) {
		_ = rc.Close()
		return nil, fmt.Errorf("repair: stage %s invalid magic %x", key, hdr[:4])
	}
	if hdr[4] != stageFormatVersion {
		_ = rc.Close()
		return nil, fmt.Errorf("repair: stage %s unsupported version %d", key, hdr[4])
	}

	sr := &StageReader{rc: rc, br: br}
	for range skipRows {
		if err := sr.skipOne(); err != nil {
			_ = rc.Close()
			return nil, fmt.Errorf("repair: skip stage %s row %d: %w", key, sr.rowIdx, err)
		}
	}
	return sr, nil
}

func (r *StageReader) skipOne() error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.br, lenBuf[:]); err != nil {
		return err
	}
	n := int64(binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.CopyN(io.Discard, r.br, n); err != nil {
		return err
	}
	r.rowIdx++
	return nil
}

// Next decodes the next staged event into dst. Returns (false, nil) at EOF.
func (r *StageReader) Next(dst *segment.Event) (bool, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(r.br, lenBuf[:])
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("repair: read stage row %d length: %w", r.rowIdx, err)
	}
	n := int(binary.BigEndian.Uint32(lenBuf[:]))
	if cap(r.rowBuf) < n {
		r.rowBuf = make([]byte, n)
	}
	buf := r.rowBuf[:n]
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return false, fmt.Errorf("repair: read stage row %d body: %w", r.rowIdx, err)
	}
	if err := decodeEvent(bytes.NewReader(buf), dst); err != nil {
		return false, fmt.Errorf("repair: decode stage row %d: %w", r.rowIdx, err)
	}
	r.rowIdx++
	return true, nil
}

// RowIndex returns the 0-based index of the next row to be read.
func (r *StageReader) RowIndex() int64 { return r.rowIdx }

// Close closes the underlying staged artifact reader.
func (r *StageReader) Close() error { return r.rc.Close() }

// FSStageStore persists staged replacement artifacts as synced files under dir
// using pebble/vfs.FS (or vfs.Default when fs is nil).
type FSStageStore struct {
	fs  vfs.FS
	dir string
}

// NewFSStageStore returns a local filesystem StageStore rooted at dir.
func NewFSStageStore(fs vfs.FS, dir string) *FSStageStore {
	if fs == nil {
		fs = vfs.Default
	}
	return &FSStageStore{fs: fs, dir: dir}
}

func (s *FSStageStore) path(key string) string {
	return filepath.Join(s.dir, filepath.Base(key))
}

func (s *FSStageStore) Create(_ context.Context, key string) (StageSink, error) {
	if err := s.fs.MkdirAll(s.dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", s.dir, err)
	}
	finalPath := s.path(key)
	tmpPath := finalPath + ".tmp"
	f, err := s.fs.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", tmpPath, err)
	}
	return &fsStageSink{
		fs:        s.fs,
		dir:       s.dir,
		tmpPath:   tmpPath,
		finalPath: finalPath,
		f:         f,
	}, nil
}

func (s *FSStageStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	return s.fs.Open(s.path(key))
}

func (s *FSStageStore) Delete(_ context.Context, key string) error {
	err := s.fs.Remove(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

type fsStageSink struct {
	fs        vfs.FS
	dir       string
	tmpPath   string
	finalPath string
	f         vfs.File
}

func (s *fsStageSink) Write(p []byte) (int, error) { return s.f.Write(p) }

func (s *fsStageSink) Commit(_ context.Context) error {
	if err := s.f.Sync(); err != nil {
		_ = s.f.Close()
		_ = s.fs.Remove(s.tmpPath)
		return fmt.Errorf("sync %s: %w", s.tmpPath, err)
	}
	if err := s.f.Close(); err != nil {
		_ = s.fs.Remove(s.tmpPath)
		return fmt.Errorf("close %s: %w", s.tmpPath, err)
	}
	if err := s.fs.Rename(s.tmpPath, s.finalPath); err != nil {
		_ = s.fs.Remove(s.tmpPath)
		return fmt.Errorf("rename %s -> %s: %w", s.tmpPath, s.finalPath, err)
	}
	if dirF, err := s.fs.OpenDir(s.dir); err == nil {
		syncErr := dirF.Sync()
		closeErr := dirF.Close()
		if syncErr != nil {
			return fmt.Errorf("sync dir %s: %w", s.dir, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close dir %s: %w", s.dir, closeErr)
		}
	}
	return nil
}

func (s *fsStageSink) Abort() error {
	_ = s.f.Close()
	err := s.fs.Remove(s.tmpPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// BlobStageStore persists staged replacement artifacts in disaggregated mode
// under <prefix><key> on objstore.Blob, outside the catalog objects/ prefix so
// object GC cannot delete in-flight repair stages.
type BlobStageStore struct {
	blob   objstore.Blob
	prefix string
}

// NewBlobStageStore returns a StageStore backed by blob under prefix.
func NewBlobStageStore(blob objstore.Blob, prefix string) *BlobStageStore {
	return &BlobStageStore{blob: blob, prefix: prefix}
}

func (s *BlobStageStore) key(k string) string {
	return s.prefix + filepath.Base(k)
}

func (s *BlobStageStore) Create(_ context.Context, key string) (StageSink, error) {
	return &blobStageSink{blob: s.blob, key: s.key(key)}, nil
}

func (s *BlobStageStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := s.blob.GetKey(ctx, s.key(key))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *BlobStageStore) Delete(ctx context.Context, key string) error {
	return s.blob.DeleteKey(ctx, s.key(key))
}

type blobStageSink struct {
	blob objstore.Blob
	key  string
	buf  bytes.Buffer
}

func (s *blobStageSink) Write(p []byte) (int, error) { return s.buf.Write(p) }

func (s *blobStageSink) Commit(ctx context.Context) error {
	data := s.buf.Bytes()
	err := s.blob.PutKey(ctx, s.key, data)
	s.buf.Reset()
	return err
}

func (s *blobStageSink) Abort() error {
	s.buf.Reset()
	return nil
}

// MemStageStore is an in-memory StageStore for tests and callers without
// filesystem or object-store staging configured.
type MemStageStore struct {
	mu    sync.Mutex
	items map[string][]byte
}

// NewMemStageStore returns an empty in-memory StageStore.
func NewMemStageStore() *MemStageStore {
	return &MemStageStore{items: make(map[string][]byte)}
}

func (s *MemStageStore) Create(_ context.Context, key string) (StageSink, error) {
	return &memStageSink{store: s, key: key}, nil
}

func (s *MemStageStore) Open(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.items[key]
	s.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *MemStageStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	delete(s.items, key)
	s.mu.Unlock()
	return nil
}

// Count returns the number of currently stored staged artifacts.
func (s *MemStageStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

type memStageSink struct {
	store *MemStageStore
	key   string
	buf   bytes.Buffer
}

func (s *memStageSink) Write(p []byte) (int, error) { return s.buf.Write(p) }

func (s *memStageSink) Commit(_ context.Context) error {
	cp := append([]byte(nil), s.buf.Bytes()...)
	s.buf.Reset()
	s.store.mu.Lock()
	s.store.items[s.key] = cp
	s.store.mu.Unlock()
	return nil
}

func (s *memStageSink) Abort() error {
	s.buf.Reset()
	return nil
}

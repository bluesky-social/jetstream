package migrate

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/time/rate"
)

// throttle bounds the migrator's reads of local segment files, so the seed
// does not starve cold reads of the same volume.
type throttle struct {
	lim *rate.Limiter
}

func newThrottle(bytesPerSec int64) *throttle {
	if bytesPerSec <= 0 {
		return &throttle{}
	}
	return &throttle{lim: rate.NewLimiter(rate.Limit(bytesPerSec), int(max(bytesPerSec, uploadChunkBytes)))}
}

func (t *throttle) wait(ctx context.Context, n int) error {
	if t.lim == nil {
		return nil
	}
	for n > 0 {
		step := min(n, t.lim.Burst())
		if err := t.lim.WaitN(ctx, step); err != nil {
			return err
		}
		n -= step
	}
	return nil
}

// segmentFile is an open local segment file.
type segmentFile struct {
	f    vfs.File
	path string
	thr  *throttle
}

func openSegment(fsys vfs.FS, path string, thr *throttle) (*segmentFile, error) {
	if fsys == nil {
		fsys = vfs.Default
	}
	f, err := fsys.Open(path)
	if err != nil {
		return nil, fmt.Errorf("migrate: open %s: %w", path, err)
	}
	return &segmentFile{f: f, path: path, thr: thr}, nil
}

func (s *segmentFile) Close() error { return s.f.Close() }

func (s *segmentFile) readAt(ctx context.Context, n int, off int64) ([]byte, error) {
	if err := s.thr.wait(ctx, n); err != nil {
		return nil, err
	}
	buf := make([]byte, n)
	if _, err := s.f.ReadAt(buf, off); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("migrate: %s: %d bytes at %d run past EOF: %w", s.path, n, off, segment.ErrCorruptSegment)
		}
		return nil, fmt.Errorf("migrate: read %s: %w", s.path, err)
	}
	return buf, nil
}

// frame reads block b's zstd frame and checks its length prefix, as the
// local fetcher does. A seal or a resumed writer never rewrites a block's
// bytes, so an active segment's offsets stay good after it is sealed.
func (s *segmentFile) frame(ctx context.Context, b segment.BlockInfo) ([]byte, error) {
	buf, err := s.readAt(ctx, 8+int(b.CompressedSize), int64(b.Offset))
	if err != nil {
		return nil, err
	}
	if n := binary.LittleEndian.Uint64(buf[:8]); n != uint64(b.CompressedSize) {
		return nil, fmt.Errorf("migrate: %s block at %d has prefix %d, index %d: %w", s.path, b.Offset, n, b.CompressedSize, segment.ErrCorruptSegment)
	}
	return buf[8:], nil
}

// sealedParts are a sealed file's header and footer, checksum-verified,
// and its block index.
type sealedParts struct {
	header, footer []byte
	hdr            segment.Header
	blocks         []segment.BlockInfo
}

// sealed reads the file's header and footer and verifies them against the
// header checksum and generation, the checksum the local catalog saw.
func (s *segmentFile) sealed(ctx context.Context, generation uint64) (sealedParts, error) {
	header, err := s.readAt(ctx, segment.ReservedHeaderBytes, 0)
	if err != nil {
		return sealedParts{}, err
	}
	hdr, err := segment.ReadSealedHeader(byteReaderAt(header))
	if err != nil {
		return sealedParts{}, fmt.Errorf("migrate: %s: %w", s.path, err)
	}
	if hdr.Checksum != generation {
		return sealedParts{}, fmt.Errorf("%w: %s holds generation %x; the catalog saw %x", errLocalChanged, s.path, hdr.Checksum, generation)
	}
	st, err := s.f.Stat()
	if err != nil {
		return sealedParts{}, fmt.Errorf("migrate: stat %s: %w", s.path, err)
	}
	if hdr.FooterOffset < segment.ReservedHeaderBytes || int64(hdr.FooterOffset) >= st.Size() {
		return sealedParts{}, fmt.Errorf("migrate: %s: footer at %d in a %d-byte file: %w", s.path, hdr.FooterOffset, st.Size(), segment.ErrCorruptSegment)
	}
	footer, err := s.readAt(ctx, int(st.Size()-int64(hdr.FooterOffset)), int64(hdr.FooterOffset))
	if err != nil {
		return sealedParts{}, err
	}
	// OpenReaderParts verifies the checksum over the header and footer and
	// validates the block index against the footer offset.
	r, err := segment.OpenReaderParts(header, footer, func(int) ([]byte, error) {
		return nil, errors.New("migrate: no block reads")
	}, segment.ReaderOptions{Name: s.path})
	if err != nil {
		return sealedParts{}, fmt.Errorf("migrate: %s: %w", s.path, err)
	}
	blocks := r.Blocks()
	_ = r.Close()
	return sealedParts{header: header, footer: footer, hdr: hdr, blocks: blocks}, nil
}

type byteReaderAt []byte

func (b byteReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// errLocalChanged means a local segment changed under the migrator. With
// compaction paused only a seal of the active segment does that, which the
// next poll sees; anything else is caught by reconcile.
var errLocalChanged = errors.New("migrate: local segment changed")

// localNext reads the source's durable seq/next: every seq below it is in
// a durable block or a registered vacancy.
func localNext(ctx context.Context, st metastore.Store) (uint64, error) {
	v, err := st.Get(ctx, []byte(catalog.MainSeqKey))
	found := true
	if errors.Is(err, metastore.ErrNotFound) {
		found, err = false, nil
	}
	if err != nil {
		return 0, fmt.Errorf("migrate: read local %s: %w", catalog.MainSeqKey, err)
	}
	n, err := catalog.DecodeSeq(catalog.MainSeqKey, v, found)
	if err != nil {
		return 0, fmt.Errorf("migrate: local %w", err)
	}
	return n, nil
}

// sameBlock reports whether two index entries describe the same block.
func sameBlock(a, b segment.BlockInfo) bool {
	return a.MinSeq == b.MinSeq && a.MaxSeq == b.MaxSeq && a.EventCount == b.EventCount &&
		a.CompressedSize == b.CompressedSize && a.UncompressedSize == b.UncompressedSize &&
		a.MinWitnessedAt == b.MinWitnessedAt && a.MaxWitnessedAt == b.MaxWitnessedAt
}

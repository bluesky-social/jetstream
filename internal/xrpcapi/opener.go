package xrpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bluesky-social/jetstream/segment"
)

// ErrSegmentNotFound is returned by SegmentOpener.OpenSegment when the
// archive has no sealed segment at the index. Handlers map it to 404
// SegmentNotFound; every other open error is a 500.
var ErrSegmentNotFound = errors.New("xrpcapi: segment not found")

// SegmentFile is one generation of one sealed segment, readable as a
// virtual file. Every byte it serves, and its Header, come from the same
// generation: a compaction that rewrites the segment after OpenSegment does
// not change what an open SegmentFile reads. getSegment and getBlock take
// their ETag (Header().Checksum) and their bytes from one SegmentFile so a
// rewrite cannot splice two generations into one response.
type SegmentFile interface {
	io.ReaderAt
	// Size is the length of the file in bytes.
	Size() int64
	// Header is the sealed header, parsed from this file's own bytes.
	Header() segment.Header
	// ModTime is the Last-Modified time served with the file.
	ModTime() time.Time
	// Content returns a reader over the whole file for http.ServeContent.
	// It may share a read position with the file, so call it at most once.
	Content() io.ReadSeeker
	Close() error
}

// SegmentOpener opens sealed segments for the archive endpoints. The
// local implementation is FileOpener; disaggregated serving (S2) opens
// the same bytes from the object store.
type SegmentOpener interface {
	OpenSegment(ctx context.Context, idx uint64) (SegmentFile, error)
}

// FileOpener opens sealed segments from the paths Src reports. Existence
// follows Src, the manifest in production, so getSegment and getBlock agree
// with listSegments and planSnapshot about which segments exist. The
// generation is pinned by the open fd: an atomic rename during compaction
// leaves the old inode readable until Close.
type FileOpener struct {
	Src SegmentSource
}

var _ SegmentOpener = FileOpener{}

func (o FileOpener) OpenSegment(_ context.Context, idx uint64) (SegmentFile, error) {
	ref, ok := o.Src.SegmentByIdx(idx)
	if !ok {
		return nil, ErrSegmentNotFound
	}
	f, err := os.Open(ref.Path)
	if err != nil {
		// The manifest believes this segment exists but it cannot be
		// opened: a real inconsistency (rotation/deletion race), so it is
		// an error rather than ErrSegmentNotFound.
		return nil, fmt.Errorf("xrpcapi: open segment %d: %w", idx, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("xrpcapi: stat segment %d: %w", idx, err)
	}
	// ReadSealedHeader validates magic/version, so a corrupt or foreign
	// file errors instead of serving a confident strong ETag.
	hdr, err := segment.ReadSealedHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("xrpcapi: read header of segment %d (%s): %w", idx, ref.Path, err)
	}
	return &localSegmentFile{File: f, size: info.Size(), modTime: info.ModTime(), hdr: hdr}, nil
}

type localSegmentFile struct {
	*os.File
	size    int64
	modTime time.Time
	hdr     segment.Header
}

func (f *localSegmentFile) Size() int64            { return f.size }
func (f *localSegmentFile) Header() segment.Header { return f.hdr }
func (f *localSegmentFile) ModTime() time.Time     { return f.modTime }

// Content returns the *os.File itself: http.ServeContent reaches sendfile(2)
// only when the copy source is the file, and any wrapper (SectionReader,
// bufio) falls back to a userspace copy.
func (f *localSegmentFile) Content() io.ReadSeeker { return f.File }

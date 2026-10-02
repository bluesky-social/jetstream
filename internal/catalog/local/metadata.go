package local

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/errors/oserror"
)

// SealedMetadata returns a Reader over sealed segment v's header and
// footer, for readers that need footer sections the view does not carry:
// block DID blooms and the collection index. Its DecodeBlock reads through
// the catalog's Fetcher, pinned to v's generation.
//
// It reports catalog.ErrStaleRef when the file no longer holds v's
// generation (a compaction rewrite or merge cleanup since the view was
// taken). Unlike Fetch it does not reload the segment: callers that retry
// take a fresh Snapshot, and the next Fetch through it reloads.
func (c *Catalog) SealedMetadata(v catalog.SegmentView) (*segment.Reader, error) {
	if v.State != catalog.Sealed {
		return nil, fmt.Errorf("catalog/local: metadata of %s segment %d in %q", v.State, v.Index, v.Namespace)
	}
	path := c.Path(v.Namespace, v.Index)
	if path == "" {
		return nil, fmt.Errorf("catalog/local: metadata of segment %d in namespace %q with no directory", v.Index, v.Namespace)
	}
	file, err := c.fs.Open(path)
	if oserror.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s is gone", catalog.ErrStaleRef, path)
	}
	if err != nil {
		return nil, fmt.Errorf("catalog/local: open %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	header := make([]byte, segment.ReservedHeaderBytes)
	if _, err := file.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("catalog/local: read header %s: %w", path, err)
	}
	if gen := binary.LittleEndian.Uint64(header[4:12]); gen != v.Generation {
		return nil, fmt.Errorf("%w: %s generation %x, view %x", catalog.ErrStaleRef, path, gen, v.Generation)
	}
	footerLen := v.Size - int64(v.Header.FooterOffset)
	if footerLen <= 0 {
		return nil, fmt.Errorf("%w: %s footer length %d", segment.ErrInvalidFooter, path, footerLen)
	}
	footer := make([]byte, footerLen)
	if _, err := file.ReadAt(footer, int64(v.Header.FooterOffset)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %s footer past EOF", segment.ErrCorruptSegment, path)
		}
		return nil, fmt.Errorf("catalog/local: read footer %s: %w", path, err)
	}

	f := c.Fetcher()
	return segment.OpenReaderParts(header, footer, func(i int) ([]byte, error) {
		if i < 0 || i >= len(v.Blocks) {
			return nil, fmt.Errorf("catalog/local: %s has no block %d", path, i)
		}
		b := v.Blocks[i]
		return f.Fetch(context.Background(), catalog.BlockRef{
			MinSeq: b.MinSeq, MaxSeq: b.MaxSeq,
			MinWitnessedUS: b.MinWitnessedAt, MaxWitnessedUS: b.MaxWitnessedAt,
			Namespace: v.Namespace, Segment: v.Index, Block: i, Generation: v.Generation,
			Loc: catalog.FileBlock{Path: path, Offset: b.Offset, Length: b.CompressedSize},
		})
	}, segment.ReaderOptions{Name: path})
}

// Package segment — Inspection surface used by the inspect-segment
// CLI. Active-file support lives in this same file; both paths
// produce the same Inspection value so the renderer is one code
// path.
package segment

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Inspection is the unified active+sealed view of a segment file
// produced by Inspect. All offset/size fields are zero where they
// don't apply (e.g. footer-section sizes on an active file).
type Inspection struct {
	Path     string
	FileSize int64
	Sealed   bool

	// Header is fully populated when Sealed. For active files the
	// fields are zero; the ones that are still meaningful (block
	// counts, seq/witnessed_at ranges, etc.) live on the dedicated
	// fields below.
	Header Header

	// Blocks is the per-block info. Sealed: parsed from the on-disk
	// block index (cheap). Active: built by walking framed blocks
	// (decompresses every block).
	Blocks []BlockInfo

	// Collections is the segment's NSID string table. For sealed
	// files this comes from the on-disk collection index. For active
	// files it's the table accumulated during the block walk in
	// first-seen order (and is therefore stable as long as the writer
	// hasn't appended more events since the inspect started).
	Collections []string
	// BlockCollections[i] is the sorted collection IDs in block i.
	BlockCollections [][]uint32
	// CollectionEventCounts[i] is the total event count for Collections[i]
	// across the whole segment. Events with empty Collection are not
	// counted, so sum(CollectionEventCounts) <= TotalEvents.
	CollectionEventCounts []uint32

	// Aggregates derived during inspection. For sealed files these
	// match the corresponding Header fields; for active files they
	// come from the block walk.
	TotalEvents    uint64
	UniqueDIDCount uint32
	MinSeq, MaxSeq uint64
	MinWitnessedAt int64
	MaxWitnessedAt int64

	// Footer-section sizes; zero for active files.
	BlockIndexBytes      uint64
	SegmentBloomBytes    uint64
	BlockBloomsBytes     uint64
	CollectionIndexBytes uint64

	// PerBlockBloomBytes is the size in bytes of each per-block
	// bloom filter. Zero for active files and for sealed files with
	// zero blocks.
	PerBlockBloomBytes uint32

	// ChecksumValid is true only when Sealed and the recomputed
	// xxh3 over header[12:]||footer matched the value in the header
	// checksum field. False on mismatch — Inspect surfaces the
	// mismatch rather than failing.
	ChecksumValid bool

	// PartialError is populated when the active-file frame walk hit
	// a torn tail or decode error. Inspection is still returned with
	// everything that could be parsed up to the failure.
	PartialError error
}

// Inspect parses the segment file at path and returns a single
// snapshot suitable for the inspect-segment renderer. Inspect handles
// both sealed and active files and does its own checksum verification
// (rather than relying on Reader.Open's reject path) so corrupted-
// but-parsable files can still be inspected.
//
// Returns a non-nil error only when the file cannot be opened, is
// shorter than the 256-byte reserved header, or does not start with
// the 'jss0' magic. A torn tail in an active file is reported via
// Inspection.PartialError.
func Inspect(path string) (*Inspection, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("segment: stat %s: %w", path, err)
	}
	fileSize := stat.Size()
	if fileSize < int64(ReservedHeaderBytes) {
		return nil, fmt.Errorf("%w: %s is %d bytes (need >= %d for header)",
			ErrCorruptSegment, path, fileSize, ReservedHeaderBytes)
	}

	headerBytes := make([]byte, ReservedHeaderBytes)
	if _, err := f.ReadAt(headerBytes, 0); err != nil {
		return nil, fmt.Errorf("segment: read header: %w", err)
	}
	if string(headerBytes[0:4]) != string(segmentMagic) {
		return nil, fmt.Errorf("%w: bad magic %q (want %q)",
			ErrCorruptSegment, headerBytes[0:4], segmentMagic)
	}

	checksum := binary.LittleEndian.Uint64(headerBytes[4:12])
	if checksum == 0 {
		return inspectActive(path, f, fileSize)
	}
	return inspectSealed(path, fileSize, headerBytes)
}

func inspectSealed(path string, fileSize int64, headerBytes []byte) (*Inspection, error) {
	header, err := decodeHeader(headerBytes)
	if err != nil {
		return nil, err
	}

	// Open via the public Reader, but skip the checksum check there
	// so we can compute it ourselves and surface the result.
	r, err := Open(ReaderConfig{Path: path, SkipChecksum: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	blocks := r.Blocks()
	collections := r.Collections()
	perBlockCollections := make([][]uint32, len(blocks))
	for i := range blocks {
		ids, err := r.BlockCollections(i)
		if err != nil {
			return nil, err
		}
		perBlockCollections[i] = ids
	}
	collectionEventCounts := r.CollectionEventCounts()

	checksumValid, err := verifySealedChecksum(path, fileSize, headerBytes, header)
	if err != nil {
		return nil, err
	}

	blockIndexBytes := header.DIDBloomOffset - header.BlockIndexOffset
	segmentBloomBytes := header.BlockDIDBloomOffset - header.DIDBloomOffset
	blockBloomsBytes := header.CollectionIndexOffset - header.BlockDIDBloomOffset
	collectionIndexBytes := uint64(fileSize) - header.CollectionIndexOffset

	var perBlockBloomBytes uint32
	if header.BlockCount > 0 && blockBloomsBytes >= blockBloomsRegionHeaderSize {
		perBlockBloomBytes = uint32((blockBloomsBytes - blockBloomsRegionHeaderSize) / uint64(header.BlockCount))
	}

	return &Inspection{
		Path:                  path,
		FileSize:              fileSize,
		Sealed:                true,
		Header:                header,
		Blocks:                blocks,
		Collections:           collections,
		BlockCollections:      perBlockCollections,
		CollectionEventCounts: collectionEventCounts,
		TotalEvents:           uint64(header.EventCount),
		UniqueDIDCount:        header.UniqueDIDCount,
		MinSeq:                header.MinSeq,
		MaxSeq:                header.MaxSeq,
		MinWitnessedAt:        header.MinWitnessedAt,
		MaxWitnessedAt:        header.MaxWitnessedAt,
		BlockIndexBytes:       blockIndexBytes,
		SegmentBloomBytes:     segmentBloomBytes,
		BlockBloomsBytes:      blockBloomsBytes,
		CollectionIndexBytes:  collectionIndexBytes,
		PerBlockBloomBytes:    perBlockBloomBytes,
		ChecksumValid:         checksumValid,
	}, nil
}

// verifySealedChecksum recomputes the xxh3 over header[12:]||footer
// and reports whether it matches the value embedded in the header.
func verifySealedChecksum(path string, fileSize int64, headerBytes []byte, header Header) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return false, fmt.Errorf("segment: reopen for checksum: %w", err)
	}
	defer func() { _ = f.Close() }()

	footerLen := fileSize - int64(header.FooterOffset)
	if footerLen <= 0 {
		return false, fmt.Errorf("%w: footer length is %d", ErrInvalidFooter, footerLen)
	}
	footerBytes := make([]byte, footerLen)
	if _, err := f.ReadAt(footerBytes, int64(header.FooterOffset)); err != nil {
		return false, fmt.Errorf("segment: read footer for checksum: %w", err)
	}

	headerForHash := make([]byte, ReservedHeaderBytes)
	copy(headerForHash, headerBytes)
	for i := 4; i < 12; i++ {
		headerForHash[i] = 0
	}
	got := xxh3HeaderFooter(headerForHash, footerBytes)
	return got == header.Checksum, nil
}

// inspectActive walks the framed-block region of an active (unsealed)
// file. Returns a populated Inspection plus, on a torn tail or decode
// failure, a non-nil PartialError.
func inspectActive(path string, f *os.File, fileSize int64) (*Inspection, error) {
	walk, walkErr := walkActiveFrames(f, fileSize)
	ins := &Inspection{
		Path:                  path,
		FileSize:              fileSize,
		Sealed:                false,
		Blocks:                walk.infos,
		Collections:           walk.collectionStringTable,
		BlockCollections:      walk.perBlockCollections,
		CollectionEventCounts: walk.collectionEventCounts,
		TotalEvents:           uint64(walk.totalEventCount),
		UniqueDIDCount:        uint32(len(walk.uniqueDIDs)),
		MinSeq:                walk.minSeq,
		MaxSeq:                walk.maxSeq,
		MinWitnessedAt:        walk.minWitnessedAt,
		MaxWitnessedAt:        walk.maxWitnessedAt,
	}
	if walkErr != nil {
		ins.PartialError = walkErr
	}
	return ins, nil
}

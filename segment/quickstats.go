package segment

// Stats is the cheap aggregate-size view of a segment file.
// Used by the status page to sum compressed and uncompressed bytes
// across an entire segment tree without decompressing blocks.
type Stats struct {
	Path              string
	FileSize          int64
	Sealed            bool
	CompressedBytes   int64
	UncompressedBytes int64
}

// QuickStats uses Inspect to total compressed and uncompressed block sizes
// without decompressing data. Sealed files use their block index; active
// files require a frame walk.
func QuickStats(path string) (Stats, error) {
	ins, err := Inspect(path)
	if err != nil {
		return Stats{}, err
	}
	var comp, uncomp int64
	for _, b := range ins.Blocks {
		comp += int64(b.CompressedSize)
		uncomp += int64(b.UncompressedSize)
	}
	return Stats{
		Path:              path,
		FileSize:          ins.FileSize,
		Sealed:            ins.Sealed,
		CompressedBytes:   comp,
		UncompressedBytes: uncomp,
	}, nil
}

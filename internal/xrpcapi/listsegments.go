package xrpcapi

import (
	"context"
	"strconv"

	"github.com/bluesky-social/jetstream/api/jetstream"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/jcalabro/atmos/xrpcserver"
	"github.com/jcalabro/gt"
)

const (
	listDefaultLimit = 100
	listMaxLimit     = 1000
)

func newListSegmentsHandler(src SegmentSource) xrpcserver.Handler {
	return xrpcserver.Query(func(ctx context.Context, p xrpcserver.Params) (*jetstream.JetstreamListSegments_Output, error) {
		limit := listDefaultLimit
		if p.Has("limit") {
			v, err := p.Int64("limit")
			if err != nil {
				return nil, err // 400 for a present but non-integer limit
			}
			if v <= 0 {
				return nil, xrpcserver.InvalidRequest("limit must be positive")
			}
			limit = int(v)
		}
		if limit > listMaxLimit {
			limit = listMaxLimit
		}

		var startIdx uint64
		if cur := p.StringOr("cursor", ""); cur != "" {
			parsed, err := strconv.ParseUint(cur, 10, 64)
			if err != nil {
				return nil, xrpcserver.InvalidRequest("invalid cursor")
			}
			startIdx = parsed
		}

		entries, nextIdx, more := src.ListFrom(startIdx, limit)

		out := &jetstream.JetstreamListSegments_Output{
			Segments: make([]jetstream.JetstreamListSegments_Segment, 0, len(entries)),
		}
		for _, e := range entries {
			out.Segments = append(out.Segments, jetstream.JetstreamListSegments_Segment{
				Name:         ingest.SegmentFilename(e.Idx),
				Index:        int64(e.Idx),
				SizeBytes:    e.SizeBytes,
				Checksum:     checksumHex(e.Checksum),
				EventCount:   int64(e.EventCount),
				MinSeq:       int64(e.MinSeq),
				MaxSeq:       int64(e.MaxSeq),
				MinIndexedAt: e.MinIndexedAt,
				MaxIndexedAt: e.MaxIndexedAt,
			})
		}
		if more {
			out.Cursor = gt.Some(strconv.FormatUint(nextIdx, 10))
		}
		return out, nil
	})
}

package overlay

import (
	"encoding/binary"
	"fmt"
	"sort"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/tombstone"
)

// realisticSnapshot builds n record tombstones with a Zipfian-ish DID skew
// (a few hot DIDs), a small set of hot collections, and TID-shaped rkeys.
func realisticSnapshot(n int, w uint64) (tombstone.Snapshot, uint64) {
	colls := []string{"app.bsky.feed.like", "app.bsky.feed.post", "app.bsky.graph.follow", "app.bsky.feed.repost"}
	snap := tombstone.Snapshot{Records: map[tombstone.RecordKey]uint64{}, DIDs: map[string]tombstone.DIDTombstone{}}
	m := w
	r := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 { r ^= r >> 12; r ^= r << 25; r ^= r >> 27; return r * 2685821657736338717 }
	nDID := n/20 + 1
	for i := 0; i < n; i++ {
		didN := next() % uint64(nDID)
		if next()%3 == 0 { // skew: a third land on the 16 hottest DIDs
			didN %= 16
		}
		did := fmt.Sprintf("did:plc:%013x", didN)
		rkey := fmt.Sprintf("3k%011x", next()%0xffffffffff)
		seq := w + 1 + next()%uint64(n*4+1)
		if seq > m {
			m = seq
		}
		snap.Records[tombstone.RecordKey{DID: did, Collection: colls[next()%uint64(len(colls))], Rkey: rkey}] = seq
	}
	return snap, m
}

// encodeFlat is the alternative layout: sorted length-prefixed rows, zstd.
func encodeFlat(snap tombstone.Snapshot, w, m uint64) []byte {
	type row struct {
		did, coll, rkey string
		seq             uint64
	}
	rows := make([]row, 0, len(snap.Records))
	for k, seq := range snap.Records {
		rows = append(rows, row{k.DID, k.Collection, k.Rkey, seq})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].did != rows[j].did {
			return rows[i].did < rows[j].did
		}
		if rows[i].coll != rows[j].coll {
			return rows[i].coll < rows[j].coll
		}
		return rows[i].rkey < rows[j].rkey
	})
	var body []byte
	for _, r := range rows {
		body = appendUvarint(body, uint64(len(r.did)))
		body = append(body, r.did...)
		body = appendUvarint(body, uint64(len(r.coll)))
		body = append(body, r.coll...)
		body = appendUvarint(body, uint64(len(r.rkey)))
		body = append(body, r.rkey...)
		body = appendUvarint(body, r.seq)
	}
	frame := overlayEncoder.EncodeAll(body, nil)
	hdr := make([]byte, frameHdrSize)
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(len(frame)))
	return append(hdr, frame...)
}

func BenchmarkEncodeColumnarVsFlat(b *testing.B) {
	for _, n := range []int{1_000, 100_000, 1_000_000} {
		snap, m := realisticSnapshot(n, 1_000_000)
		b.Run(fmt.Sprintf("columnar/%d", n), func(b *testing.B) {
			var sz int
			for i := 0; i < b.N; i++ {
				sz = len(Encode(snap, 1_000_000, m))
			}
			b.ReportMetric(float64(sz), "wire_bytes")
			b.ReportMetric(float64(sz)/float64(n), "bytes/tombstone")
		})
		b.Run(fmt.Sprintf("flat/%d", n), func(b *testing.B) {
			var sz int
			for i := 0; i < b.N; i++ {
				sz = len(encodeFlat(snap, 1_000_000, m))
			}
			b.ReportMetric(float64(sz), "wire_bytes")
			b.ReportMetric(float64(sz)/float64(n), "bytes/tombstone")
		})
	}
}

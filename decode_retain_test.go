package jetstream

import (
	"testing"
	"unsafe"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

// blockRows builds rows whose strings and payloads all alias one backing
// buffer, the way segment.DecodeBlockFrame aliases a decompressed block.
func blockRows(t *testing.T) ([]byte, []segment.Event) {
	t.Helper()
	record, err := cbor.Marshal(map[string]any{"$type": "app.bsky.feed.post", "text": "hello world"})
	require.NoError(t, err)
	id := comatproto.SyncSubscribeRepos_Identity{DID: "did:plc:alice", Handle: gt.Some("alice.test"), Seq: 7, Time: "2026-01-01T00:00:00Z"}
	identity, err := id.MarshalCBOR()
	require.NoError(t, err)

	const did, coll, rkey, rev = "did:plc:alice", "app.bsky.feed.post", "3kabc", "3kxyz"
	block := []byte(did + coll + rkey + rev)
	block = append(block, record...)
	block = append(block, identity...)
	str := unsafe.String(&block[0], len(block))
	strs := func(off int) (string, string, string, string) {
		d := str[off : off+len(did)]
		off += len(did)
		c := str[off : off+len(coll)]
		off += len(coll)
		k := str[off : off+len(rkey)]
		off += len(rkey)
		return d, c, k, str[off : off+len(rev)]
	}
	d, c, k, r := strs(0)
	recStart := len(did) + len(coll) + len(rkey) + len(rev)
	idStart := recStart + len(record)
	return block, []segment.Event{
		{Seq: 1, Kind: segment.KindCreate, DID: d, Collection: c, Rkey: k, Rev: r, Payload: block[recStart:idStart:idStart]},
		{Seq: 2, Kind: segment.KindIdentity, DID: d, Payload: block[idStart:]},
	}
}

// eventPointers lists the memory every reference in ev points at.
func eventPointers(ev Event) []uintptr {
	var ptrs []uintptr
	addStr := func(s string) {
		if s != "" {
			ptrs = append(ptrs, uintptr(unsafe.Pointer(unsafe.StringData(s))))
		}
	}
	addStr(ev.DID)
	if c := ev.Commit; c != nil {
		addStr(c.Collection)
		addStr(c.Rkey)
		addStr(c.Rev)
		if len(c.RecordCBOR) > 0 {
			ptrs = append(ptrs, uintptr(unsafe.Pointer(&c.RecordCBOR[0])))
		}
		for _, v := range c.Record {
			if s, ok := v.(string); ok {
				addStr(s)
			}
		}
	}
	if id := ev.Identity; id != nil {
		addStr(id.DID)
		addStr(id.Handle)
		addStr(id.Time)
	}
	return ptrs
}

// TestArchiveEventsDoNotPinBlock pins that a decoded archive Event owns its
// memory unless the caller opted into WithRawRecords. Before, DID, Rev,
// Collection, Rkey, marker fields, and the default Record map aliased the
// decompressed block, so a consumer that kept one Rev per DID pinned every
// block it came from (8 GB of live heap for a 40-DID backfill in prod).
func TestArchiveEventsDoNotPinBlock(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mode    recordDecodeMode
		aliases bool
	}{
		"default":       {mode: recordDecodeMode{}},
		"raw copied":    {mode: recordDecodeMode{raw: true, copyCBOR: true, wantCIDs: true}},
		"raw zero-copy": {mode: recordDecodeMode{raw: true}, aliases: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			block, rows := blockRows(t)
			lo := uintptr(unsafe.Pointer(&block[0]))
			hi := lo + uintptr(len(block))
			d := newDownloader(nil, 1, nil)
			d.setRecordMode(tc.mode)
			events, err := d.decodeRows(rows)
			require.NoError(t, err)
			require.Len(t, events, len(rows))
			inBlock := 0
			for _, ev := range events {
				for _, p := range eventPointers(ev) {
					if p >= lo && p < hi {
						inBlock++
					}
				}
			}
			if tc.aliases {
				require.Positive(t, inBlock, "WithRawRecords stays zero-copy")
			} else {
				require.Zero(t, inBlock, "event references must not point into the block")
			}
		})
	}
}

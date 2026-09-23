// Package jetstream is the official Go client for Jetstream v2, atproto's
// full-network archive and live-streaming service.
//
// A single Client can follow the live firehose, replay full or filtered
// history, or return a point-in-time archive snapshot. A replay reads the
// sealed archive and then cuts over to the live tail without a gap. Events are
// delivered as decoded, JSON-shaped Go values through a range-over-func
// iterator:
//
//	client, err := jetstream.Subscribe("jetstream.us-west.bsky.network",
//		jetstream.WithCollections([]string{"app.bsky.feed.post"}),
//		jetstream.WithAfterSeq(0), // replay from the start of the archive
//	)
//	if err != nil {
//		// handle err
//	}
//	defer client.Close()
//
//	for batch, err := range client.Events(ctx) {
//		if err != nil {
//			continue // handle error; iteration continues unless ctx is done
//		}
//		if err := db.WriteBatch(batch.Events()); err != nil {
//			continue // handle error
//		}
//		if err := db.SaveCursor(batch.LastCursor()); err != nil {
//			continue // handle error
//		}
//	}
//
// Subscribe(host) follows the current live tip. WithAfterSeq enables archive
// replay before v2 cutover; WithSnapshotOnly stops after sealed history, and
// WithBeforeSeq bounds that snapshot. Both sources yield the same Event type.
//
// WithAPIKey authenticates planSnapshot, getSegment, and getBlock only.
// Dictionary requests and the live websocket remain public. Live compression
// defaults to dictionary-zstd; use WithZstdCompression(false) to opt out.
// Dictionary failures fall back to uncompressed streaming.
//
// Delivery is at-least-once. Consumers must fold events idempotently: creates
// and updates apply; deletes, account deletions, and syncs remove records.
// Collection filters preserve DID-level markers unless kinds excludes them.
// The library delivers markers without folding or suppressing records.
//
// If cutover falls behind the server's lookback window, the client replays
// from the last processed seq. Seqs increase but need not be contiguous:
// crash recovery may leave registered vacancies to prevent reuse of
// previously observed seqs. The client accepts these jumps and deduplicates
// by seq.
//
// Transport, planning, download, and cutover implementations are private to
// this package.
package jetstream

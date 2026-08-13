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
// A bare Subscribe(host) starts a pure live tail from the current tip.
// Supplying WithAfterSeq starts a replay: the client pages planSnapshot over
// the sealed archive, downloads matching data with getSegment or getBlock, and
// then connects /xrpc/network.bsky.jetstream.subscribeEvents at the cutover
// cursor to consume the active segment and follow the live tail. Use
// WithSnapshotOnly to stop after the sealed range; WithBeforeSeq can bound that
// snapshot. There is no client-side cutover buffer or record suppression.
// Archives that require bearer authentication can use
// WithAPIKey with the raw key; it authenticates planSnapshot,
// getSegment, and getBlock only. The public dictionary request and live
// WebSocket remain unauthenticated. The live tail uses the server's
// dictionary-zstd compression by default; use WithZstdCompression(false) to opt
// out. Dictionary fetch or rotation failure degrades to an uncompressed tail
// rather than failing delivery.
//
// Delivery is at-least-once and the contract is eventually-consistent: the
// caller must process events idempotently and FOLD the stream (creates/updates
// apply; deletes, account-deletes, and syncs remove). A record deleted or
// updated after it was first delivered arrives as its own later event, exactly
// as on the upstream firehose; deleted-account markers (#account/#identity/
// #sync) are always delivered (even under a collection filter) so a folding
// consumer can purge the dead account's records. If the live cursor ages below
// the server's lookback window during a slow handoff, the client transparently
// replays the missing archive range from its last processed seq rather than
// silently skipping the gap.
//
// The client deliberately exposes a minimal public surface: the Client, its
// options, and the decoded Event shape. Transport, planning, download, and
// cutover machinery is unexported within this package.
package jetstream

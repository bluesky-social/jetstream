// Package jetstream is the official Go client for Jetstream v2, atproto's
// full-network archive and live-streaming service.
//
// A single Client can live-tail the firehose, complete a full or filtered
// historical backfill, or do both: backfill the sealed archive and then cut
// over to the live tail with no gap and no duplicated work visible to the
// caller. Events are delivered as decoded, JSON-shaped Go values through a
// range-over-func iterator:
//
//	client, err := jetstream.Subscribe("jetstream.us-west.bsky.network",
//		jetstream.WithCollections([]string{"app.bsky.feed.post"}),
//		jetstream.WithAfterSeq(0), // backfill from the start of the archive
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
// A bare Subscribe(host) with no backfill options is a pure live tail from the
// current tip. Supplying WithAfterSeq or WithBeforeSeq triggers the full
// archive-negotiation path: the client pages planBackfill over the sealed
// archive (downloading every matching sealed segment), then connects /subscribe
// once at the sealed tip to pick up the active segment and the live tail. There
// is no client-side buffer and no record suppression.
//
// Delivery is at-least-once and the contract is eventually-consistent: the
// caller must process events idempotently and FOLD the stream (creates/updates
// apply; deletes, account-deletes, and syncs remove). A record deleted or
// updated after it was first delivered arrives as its own later event, exactly
// as on the upstream firehose; deleted-account markers (#account/#identity/
// #sync) are always delivered (even under a collection filter) so a folding
// consumer can purge the dead account's records. If the live cursor ages below
// the server's lookback window during a slow handoff, the client transparently
// re-backfills from its last processed seq rather than silently skipping the gap.
//
// The client deliberately exposes a minimal public surface: the Client, its
// options, and the decoded Event shape. All transport, planning, download, and
// cutover machinery lives in internal packages.
package jetstream

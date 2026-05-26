# Subscribe Encoder Golden Fixtures

`golden_v1.jsonl` is a hand-picked set of events captured from a real
Jetstream v1 server. Each line is one JSON event exactly as it arrived
on the wire, terminated by a single `\n`.

The encoder's golden test asserts that for each event, building the
equivalent `segment.Event` and running it through `Encode` produces
JSON that is **semantically equivalent** to the original — same
`json.Unmarshal` shape, modulo key order.

## How this corpus was captured

```
websocat wss://jetstream1.us-east.bsky.network/subscribe \
  > /tmp/jetstream-v1.json
```

Then ten events were selected to exercise the full encoder surface:

| # | kind     | operation | collection                          | notes                                |
|---|----------|-----------|-------------------------------------|--------------------------------------|
| 1 | commit   | create    | app.bsky.feed.like                  | small record, `subject` w/ cid+uri   |
| 2 | commit   | create    | app.bsky.feed.post                  | text + reply (parent+root cid+uri)   |
| 3 | commit   | create    | app.bsky.feed.repost                | `subject.cid` `$link` path           |
| 4 | commit   | create    | app.bsky.graph.follow               | `subject` as plain DID string        |
| 5 | commit   | update    | app.bsky.actor.profile              | embedded blobs (`$link` + `mimeType`)|
| 6 | commit   | delete    | org.hypercerts.claim.activity       | no record body                       |
| 7 | commit   | update    | place.stream.broadcast.origin       | non-bsky lexicon, % escaping in URL  |
| 8 | identity | -         | -                                   |                                      |
| 9 | account  | -         | -                                   | `active: true`                       |
| 10| commit   | create    | app.bsky.graph.block                | diversity                            |

## Refreshing

To rotate fixtures (e.g. as v1 retires):

1. Capture a fresh batch via `websocat` as above.
2. Pick 10 events covering the matrix in the table.
3. Replace `golden_v1.jsonl` with the new selection.
4. Re-run `just test ./internal/subscribe/` and inspect any diff.

If a specific record's CID cannot be reproduced from a JSON-CBOR
round-trip (canonical DAG-CBOR encoding is sensitive to integer form
and map key order), drop that record and pick a different one. The
encoder fuzz target plus the surviving fixtures together cover the
correctness invariants we care about.

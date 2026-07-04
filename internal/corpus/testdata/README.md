# Real-Data Corpus Fixtures

These fixtures are real atproto network bytes with expected outputs
pinned by **foreign implementations** — never by
`github.com/jcalabro/atmos`, the protocol library under test. They are
the oracle's independence check against symmetric atmos bugs
(`specs/oracle.md` → "Real-Data Corpus Tier", issue #32).

All tests over these fixtures are offline: nothing in `internal/corpus`
touches the network.

## Files

| file | contents | expected side pinned by |
|---|---|---|
| `frames.bin.zst` | 150 contiguous raw relay websocket frames (`wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos`), zstd of `[u32 LE length][frame bytes]…`, byte-for-byte as received | — (input) |
| `expected_v1.jsonl` | the same events as served concurrently by production Jetstream v1 (`wss://jetstream1.us-east.bsky.network/subscribe`), one JSON line per v1-visible event, verbatim | production Jetstream v1 (TypeScript-lineage implementation, independent deployment) |
| `did_docs.json.zst` | zstd JSON object: DID → verbatim DID document for every `#commit` DID in the window, fetched from `plc.directory` at capture time | plc.directory |
| `manifest.json` | capture metadata: URLs, timestamp, seq range, per-kind event counts (used as anti-vacuity assertions) | capture tool |
| `repo.car` | production `getRepo` CAR for `did:plc:53iucmf5ldftgqyf2ac7hmhd` (a maintainer-owned account), fetched via `goat repo export` | — (input) |
| `car_expected_records.tsv` | `collection/rkey<TAB>record-CID` listing of `repo.car` | `goat repo ls` (bluesky-social/indigo MST walk + CID derivation) |
| `car_expected_commit.txt` | commit DID / data CID / rev of `repo.car` | `goat repo inspect` |
| `golden_corpus_segment.jss` | sealed segment file produced from `repo.car`'s records with pinned seqs/timestamps | a known-good jetstream build (byte-pinned; regenerate only for intentional format changes with `go test ./internal/corpus -run TestCorpusSegmentGolden -update`) |

## Provenance

Captured 2026-07-04 (see `manifest.json` for the exact timestamp and
seq range) with a **throwaway capture tool** built exclusively on
`github.com/bluesky-social/indigo` (v0.0.0-20260629160527-dfe5578fd537)
and `gorilla/websocket` — deliberately not on atmos, and deliberately
not committed to this repo (indigo may not enter this module's
dependency graph). Its full source is preserved as a comment on
issue #32.

At capture time the tool:

1. recorded raw relay frames verbatim while concurrently recording
   production Jetstream v1 JSON;
2. selected a contiguous (no seq gaps), kind-diverse window with no
   `#sync`, no `tooBig`, no rebase frames;
3. matched every v1-visible relay event to exactly one captured v1
   line — commits by `(did, rev, collection, rkey, operation)`,
   identity/account by `(did, seq)` — rejecting any window with
   unmatched or ambiguous events;
4. fetched each commit DID's document and **verified every commit with
   indigo**: structure, MST record ops, op inversion against
   `prevData`, and the signature against the DID doc's signing key.

The CAR family was produced with `goat` (indigo's CLI,
v0.0.0-20250729223159): `goat repo export <did>`, `goat repo ls`,
`goat repo inspect`.

## Privacy, size, licensing

- The firehose window is public network data (what any relay consumer
  sees); it inevitably includes third-party public posts. The window is
  kept small (~150 frames, ~600 KiB compressed total) and its contents
  are enumerable via `manifest.json`.
- `repo.car` is a maintainer-owned account (Jim Calabro), so no
  third-party repo data is committed.
- Do not grow this corpus casually: CI runs it on every push. A larger
  optional corpus belongs in an external location wired through an
  env-gated test, not in git.

## Re-capturing

Re-capture when the protocol evolves (new event kinds, Sync 1.1
changes), when production Jetstream v1 retires, or to add an incident
regression window. Rebuild the capture tool from the source preserved
on issue #32 (or rewrite it — it is ~600 lines), keeping these
requirements:

- record relay frames **byte-for-byte** (no decode/re-encode);
- enforce seq contiguity and one-to-one v1 matching before writing;
- verify signatures/MST with an implementation that is NOT atmos;
- update `manifest.json` counts (the tests' anti-vacuity assertions
  read them) and this README's capture date;
- regenerate `golden_corpus_segment.jss` with `-update` if `repo.car`
  changed, and re-run the mutation campaign (`m009`) afterwards.

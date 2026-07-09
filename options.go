package jetstream

import (
	"log/slog"
	"net/http"
	"runtime"
)

// Option configures a Client. Options are applied in order by Subscribe.
type Option func(*config)

// config is the resolved, validated client configuration. It is private:
// callers build it exclusively through Option values.
type config struct {
	collections  []string
	dids         []string
	hasAfterSeq  bool
	afterSeq     uint64
	hasBeforeSeq bool
	beforeSeq    uint64
	backfillOnly bool
	liveCursor   uint64
	batchSize    int
	downloadConc int
	// httpClient is a caller override. nil is the sentinel for "unset":
	// the engine then builds its own per-workload jttp clients
	// (xrpc.ATProtoOpts for XRPC, xrpc.BulkDownloadOpts for bulk
	// downloads). Do not install a default here — that would collapse the
	// two-client tuning into one shared client. See WithHTTPClient.
	httpClient *http.Client
	logger     *slog.Logger
	// maxDownloadAttempts, when > 0, caps the total number of attempts
	// (initial + retries) the XRPC clients make per request. 0 (unset)
	// leaves xrpc on its default retry policy. See WithMaxDownloadAttempts.
	maxDownloadAttempts int
	// rawRecords skips building Commit.Record map[string]any (see WithRawRecords);
	// rawRecordsCopied additionally clones RecordCBOR so it is safe to retain
	// (see WithRawRecordsCopied); rawRecordCIDs keeps computing Commit.CID in raw
	// mode (see WithRawRecordCIDs).
	rawRecords       bool
	rawRecordsCopied bool
	rawRecordCIDs    bool
}

// Defaults applied when an option is not supplied.
const (
	defaultBatchSize = 64
	// maxAutoDownloadConc caps the auto-sized download concurrency. Backfill
	// throughput is decode-bound and, on the records we've measured, the decode
	// pool stops scaling well before this many workers, so a higher cap buys no
	// throughput while costing one in-flight ~segment-sized download buffer per
	// worker (the compressed-file memory term) and one HTTP connection. 32 spans
	// the measured scaling knee on big machines while staying modest on memory
	// and connection count; operators who want more set WithDownloadConcurrency.
	maxAutoDownloadConc = 32
	// minAutoDownloadConc keeps small machines from dropping to a near-serial
	// backfill: even a 2-core box should overlap a couple of downloads/decodes.
	minAutoDownloadConc = 4
)

// defaultDownloadConc auto-sizes download/decode concurrency to the machine:
// GOMAXPROCS (the cores actually available to this process, honoring cgroup
// CPU limits), clamped to [minAutoDownloadConc, maxAutoDownloadConc]. This lets
// a 256-core production host use far more of its cores out of the box than the
// old fixed default of 8, while a laptop or a CPU-limited container stays
// reasonable. WithDownloadConcurrency overrides it explicitly.
func defaultDownloadConc() int {
	n := runtime.GOMAXPROCS(0)
	if n < minAutoDownloadConc {
		return minAutoDownloadConc
	}
	if n > maxAutoDownloadConc {
		return maxAutoDownloadConc
	}
	return n
}

func defaultConfig() config {
	return config{
		batchSize:    defaultBatchSize,
		downloadConc: defaultDownloadConc(),
	}
}

// backfillRequested reports whether the caller asked for historical archive
// replay (any seq bound) versus a pure live tail.
func (c *config) backfillRequested() bool {
	return c.hasAfterSeq || c.hasBeforeSeq
}

// WithCollections restricts delivery to the given collections. Each entry is
// either an exact NSID (e.g. "app.bsky.feed.post") or a namespace wildcard
// ending in ".*" (e.g. "app.bsky.feed.*"). Empty or unset means all
// collections.
//
// A collection filter does NOT suppress DID-level events: Account, Identity,
// and Sync carry no collection but always bypass the collection filter
// (subject to WithDIDs), because they are a folding consumer's only signal to
// purge a deleted account's records — hiding them would create a permanently
// stale view. The client no longer suppresses deleted-account records during
// backfill; consumers fold those markers themselves. With no collection
// filter, Account and Identity events are likewise delivered, subject to
// WithDIDs. See issue #142.
func WithCollections(collections []string) Option {
	return func(c *config) { c.collections = append([]string(nil), collections...) }
}

// WithDIDs restricts delivery to the given DIDs. Empty or unset means all DIDs.
// The DID filter applies to every event kind, including Account and Identity:
// with a DID filter and no collection filter, you receive Account and Identity
// events for the matching DIDs only.
func WithDIDs(dids []string) Option {
	return func(c *config) { c.dids = append([]string(nil), dids...) }
}

// WithAfterSeq sets the exclusive lower sequence bound for backfill: only
// events with seq > afterSeq are delivered. Supplying it (including
// WithAfterSeq(0) to mean "from the start of the archive") enables the
// historical backfill path.
func WithAfterSeq(seq uint64) Option {
	return func(c *config) {
		c.hasAfterSeq = true
		c.afterSeq = seq
	}
}

// WithBeforeSeq sets the inclusive upper sequence bound for backfill: only
// events with seq <= beforeSeq are delivered from the archive. Enables the
// historical backfill path.
//
// It requires WithBackfillOnly: a beforeSeq is meaningful only as a bounded
// archive dump. On a backfill-then-live subscription the same upper bound would
// gate the live tail and silently drop every event past beforeSeq, so Subscribe
// rejects WithBeforeSeq unless WithBackfillOnly is also set.
func WithBeforeSeq(seq uint64) Option {
	return func(c *config) {
		c.hasBeforeSeq = true
		c.beforeSeq = seq
	}
}

// WithBackfillOnly turns the client into a one-time archive dump: it downloads
// and delivers the matched sealed range (bounded by WithAfterSeq/WithBeforeSeq)
// and then ends the stream, without ever starting the live tail or cutover.
//
// It requires a backfill bound (WithAfterSeq and/or WithBeforeSeq); without one
// there is no archive to dump and Subscribe returns an error. Records in the
// active, unsealed segment (above the sealed tip) are only reachable via the
// live tail and are therefore not delivered by a dump.
func WithBackfillOnly() Option {
	return func(c *config) { c.backfillOnly = true }
}

// WithLiveCursor resumes a pure live tail from a previously saved cursor
// (typically Batch.LastCursor from a prior run). The server delivers events
// with seq > cursor. Ignored when a backfill bound is also set, since the
// backfill path computes its own live cutover cursor.
func WithLiveCursor(seq uint64) Option {
	return func(c *config) {
		c.liveCursor = seq
	}
}

// WithBatchSize sets the maximum number of events returned in a single Batch.
// Must be > 0; ignored otherwise. Default 64.
func WithBatchSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithDownloadConcurrency sizes the backfill parallelism: n bounds the block
// decode pool directly, and the block-mode getBlock fetch pool is derived from
// it (2n, capped at 64) so sparse (DID/collection-filtered) backfills overlap
// network round trips instead of paying one RTT per block. Must be > 0;
// ignored otherwise.
//
// Whole-segment downloads are prefetched ahead of decode but currently run one
// at a time, so over a WAN this knob governs decode and sparse-fetch
// parallelism, not whole-segment wire throughput.
//
// The default is auto-sized from the CPU count (GOMAXPROCS, clamped to
// [4, 32]), so a many-core host uses more of its cores without configuration
// while small/CPU-limited environments stay modest. Set this to override the
// auto-sizing — e.g. a higher value on a very large box, or a lower value to
// cap memory (each in-flight download holds roughly one segment-sized buffer).
func WithDownloadConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.downloadConc = n
		}
	}
}

// WithHTTPClient overrides the HTTP client used for both XRPC negotiation
// and bulk segment/block downloads. It is an override: when unset, the
// client builds its own jttp clients tuned per workload — xrpc.ATProtoOpts
// for the short XRPC calls (planBackfill) and
// xrpc.BulkDownloadOpts for the streaming segment/block downloads, whose
// large transfers a short wall-clock timeout would prematurely kill.
// Supplying a client here replaces both with the single client given.
func WithHTTPClient(h *http.Client) Option {
	return func(c *config) {
		if h != nil {
			c.httpClient = h
		}
	}
}

// WithMaxDownloadAttempts caps the total number of attempts (the initial
// request plus retries) each XRPC/download request makes before failing.
// n <= 0 is ignored (leaves the default retry policy). n == 1 disables
// retries entirely.
//
// The default policy retries transient failures, which is right for
// production resilience but undesirable for tests and tools that must fail
// fast against a deliberately-broken or unavailable backend rather than
// wait out a long backoff schedule. Bounding attempts turns a permanent
// download failure into a prompt error instead of a slow retry loop.
func WithMaxDownloadAttempts(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxDownloadAttempts = n
		}
	}
}

// WithLogger sets a structured logger for diagnostics. The default discards
// all output.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithRawRecords makes commit decoding SKIP building the generic
// Commit.Record map[string]any. Instead, Commit.Record is left nil and
// Commit.RecordCBOR is populated with the record's raw DAG-CBOR bytes, which the
// caller decodes itself — typically into a typed struct via the generic
// TypedEvents helper. Building the generic map dominates decode CPU and
// allocations at scale (#142), so skipping it is the main lever for high-volume
// backfills of a single record type (e.g. app.bsky.feed.like).
//
// Deletes (no record), identity/account/sync events, and the default Commit
// fields (Operation/Collection/Rkey/Rev) are unaffected. Commit.CID is left
// empty in raw mode unless WithRawRecordCIDs is also set (computing it is real
// per-record work this fast path avoids by default).
//
// Aliasing/lifetime contract: in raw mode Commit.RecordCBOR aliases the
// client's internal decompressed buffer on the backfill path (zero-copy), valid
// only for the lifetime of the Batch that delivered it — the same contract the
// default Record already carries. Anything decoded from it that retains slices
// or strings (typed structs whose string fields alias the input) is likewise
// valid only for the batch; copy it to retain longer. Use WithRawRecordsCopied
// for a safe (cloned) variant that still skips the map build.
func WithRawRecords() Option {
	return func(c *config) { c.rawRecords = true }
}

// WithRawRecordsCopied is like WithRawRecords but Commit.RecordCBOR is a private
// copy of the record bytes (not an alias of the internal buffer), so it — and
// anything decoded from it — is safe to retain past the delivering Batch. It
// still skips the generic map build; the only cost over WithRawRecords is one
// allocation + copy of the raw bytes per commit. Use this when the consumer
// keeps records around; use WithRawRecords for maximum throughput when records
// are processed within the iteration.
func WithRawRecordsCopied() Option {
	return func(c *config) {
		c.rawRecords = true
		c.rawRecordsCopied = true
	}
}

// WithRawRecordCIDs keeps computing Commit.CID (the record's content identifier,
// a sha256 + base32 of the payload) when raw-record mode is enabled. Without it,
// raw mode leaves Commit.CID empty to avoid the per-record hashing cost. No
// effect unless WithRawRecords or WithRawRecordsCopied is also set.
func WithRawRecordCIDs() Option {
	return func(c *config) { c.rawRecordCIDs = true }
}

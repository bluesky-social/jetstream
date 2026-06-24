package jetstream

import (
	"log/slog"
	"net/http"
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
	liveBuffer   LiveBuffer
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
}

// Defaults applied when an option is not supplied.
const (
	defaultBatchSize    = 64
	defaultDownloadConc = 8
)

func defaultConfig() config {
	return config{
		batchSize:    defaultBatchSize,
		downloadConc: defaultDownloadConc,
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
// Setting a collection filter also suppresses Account and Identity events:
// they carry no collection, so a collection-scoped subscriber does not receive
// them. (Account deletions are still applied internally as tombstones, so
// records for a deleted account are correctly suppressed — you just don't see
// the Account event itself.) With no collection filter, Account and Identity
// events are delivered, subject to WithDIDs. See issue #142.
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

// WithDownloadConcurrency bounds how many sealed segment/block downloads run
// in parallel during backfill. Must be > 0; ignored otherwise. Default 8.
func WithDownloadConcurrency(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.downloadConc = n
		}
	}
}

// WithLiveBuffer supplies the buffer used to hold live-tail events received
// during the backfill-to-live cutover. The default is an in-memory buffer.
// A durable, file-backed implementation is available for long-running
// backfills; see NewFileLiveBuffer.
func WithLiveBuffer(b LiveBuffer) Option {
	return func(c *config) {
		if b != nil {
			c.liveBuffer = b
		}
	}
}

// WithHTTPClient overrides the HTTP client used for both XRPC negotiation
// and bulk segment/block downloads. It is an override: when unset, the
// client builds its own jttp clients tuned per workload — xrpc.ATProtoOpts
// for the short XRPC calls (getTombstones/planBackfill) and
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

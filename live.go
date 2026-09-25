package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/seqspace"
	"github.com/bluesky-social/jetstream/internal/zstddict"
	"github.com/coder/websocket"
	"github.com/klauspost/compress/zstd"
)

const (
	// defaultLiveReadLimit bounds a single websocket message. v2 frames
	// carry base64 record CBOR; a generous ceiling tolerates large records
	// without allowing an unbounded allocation from a hostile server.
	defaultLiveReadLimit = 32 << 20 // 32 MiB

	// reconnect backoff bounds.
	liveBackoffMin = 250 * time.Millisecond
	liveBackoffMax = 30 * time.Second
)

// wsConn is the subset of *websocket.Conn the consumer uses, extracted so
// tests can substitute a fake transport without a real socket.
type wsConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Close(code websocket.StatusCode, reason string) error
	SetReadLimit(limit int64)
}

// dialFunc establishes a live websocket connection to url. Tests inject a
// fake; production uses dialWebsocket.
type dialFunc func(ctx context.Context, url string) (wsConn, error)

// liveConfig configures a liveConsumer.
type liveConfig struct {
	host string // normalized base URL, e.g. "https://host"
	// failoverHosts are normalized fallbacks tried round-robin after host when
	// a dial fails. Honored only under timeMode: a seq cursor means nothing on
	// another instance.
	failoverHosts []string
	// timeMode enables witnessed-time resume across seq namespaces: a
	// reconnect that cannot prove it reached the same process (boot ID) or
	// that fails over resumes at lastWitnessed-rewind instead of lastSeq.
	timeMode bool
	rewind   time.Duration
	// witnessedFloor seeds lastWitnessed: the highest witnessed_at the caller
	// already holds (the backfill's), so a failover before the first live
	// delivery still has a time to resume from.
	witnessedFloor int64
	// archiveNS marks dedupFloor as a seq in the primary's archive namespace,
	// the only namespace a CursorTooOld may send back to a re-backfill.
	archiveNS bool
	// cursor is the initial WIRE resume point sent as ?cursor= on the first
	// connection: either a seq or a legacy unix-microsecond timestamp. cursor=0
	// means "replay from the beginning" (everything, since the first real event
	// is seq 1). A seq resume is inclusive; the consumer's own seq dedup turns it
	// into the effective "> last delivered". A timestamp is only a server-side
	// seek position and therefore has dedupFloor 0. Ignored when fromTip is set.
	cursor uint64
	// fromTip, when true, omits the ?cursor= param so the server starts at the
	// live tip with no replay. This is the WithLiveCursor(0) "live from tip"
	// user-API contract — distinct from cursor=0 ("replay everything"). Only the
	// pure live-only path sets it; the cutover always sends an explicit cursor.
	fromTip bool
	// kinds, collections, and dids are the caller's canonical filters, forwarded
	// as repeated v2 query params so the server can prune before encoding and
	// transmission. Empty means "no filter" (the param is omitted).
	// The client-side matcher remains a correctness backstop.
	kinds       []Kind
	collections []string
	dids        []string
	// dedupFloor seeds lastSeq: the highest seq the caller already holds, so the
	// at-least-once re-delivery at or below it is dropped. 0 means "nothing
	// delivered yet", so the first real event (seq >= 1) always passes — the
	// seq-0 swallow is structurally impossible under 1-based seqs (design §R8).
	dedupFloor uint64
	readLimit  int64
	dial       dialFunc
	httpClient *http.Client // optional; routes the live websocket upgrade through a custom transport
	logger     *slog.Logger
	// backoffMin/backoffMax override the reconnect backoff bounds. Zero uses
	// the package defaults. Tests set tiny values to avoid real-time waits.
	backoffMin time.Duration
	backoffMax time.Duration
	// mode selects raw vs. map record materialization for live commits. Zero
	// value = the default map build.
	mode recordDecodeMode
	// zstdDict, when non-nil, opts the connection into the
	// dict-zstd compression scheme: the dictionary ID (parsed from the
	// blob's header) is sent as ?zstdDictionary=<id> and incoming BINARY
	// frames are decompressed with it. The caller obtains the blob via
	// getZstdDictionary before constructing the consumer. nil = plain
	// uncompressed text frames (v2 never negotiates permessage-deflate).
	zstdDict []byte
	// refetchDict, when non-nil, re-fetches the server's CURRENT dictionary
	// after the server rejects the pinned ID (a dictionary rotation: retrain
	// + redeploy changes DictionaryV2ID while this consumer holds the old
	// blob). host is the instance being dialed, since dictionaries are per
	// instance. nil disables in-place recovery; the consumer then degrades to
	// an uncompressed tail on rejection. See refreshDict.
	refetchDict func(ctx context.Context, host string) []byte
}

func (c liveConfig) minBackoff() time.Duration {
	if c.backoffMin > 0 {
		return c.backoffMin
	}
	return liveBackoffMin
}

func (c liveConfig) maxBackoff() time.Duration {
	if c.backoffMax > 0 {
		return c.backoffMax
	}
	return liveBackoffMax
}

// liveConsumer tails the live v2 websocket, decoding frames into
// engine events, deduplicating the at-least-once overlap by seq, and
// reconnecting with bounded exponential backoff. It is the live half of the
// stream: the engine consumes its output during cutover (buffered) and in
// steady state (direct).
type liveConsumer struct {
	cfg liveConfig
	// lastSeq is the highest seq delivered, used both as the dedup floor and the
	// reconnect resume cursor. 0 means "nothing delivered yet" (the first real
	// event is seq >= 1, so it passes the ev.Seq <= lastSeq dedup); a positive
	// value resumes/dedups above it. seenAny disambiguates a from-tip start
	// (lastSeq 0, nothing delivered → omit the wire cursor on reconnect) from a
	// replay-from-0 start.
	lastSeq uint64
	seenAny bool
	// delivered counts emitted events; a change across a session means it made
	// progress (lastSeq cannot say so once a failover resets it).
	delivered uint64

	// Failover state, used only under cfg.timeMode. hosts[0] is cfg.host.
	// hasSeqPos means lastSeq (or the configured seq start) is a usable
	// position in the namespace of hosts[nsHost], served by the process
	// nsBoot ("" = not yet learned). lastWitnessed is the witnessed_at of the
	// latest delivery, the resume point once the namespace is abandoned.
	hosts         []string
	hostIdx       int
	hasSeqPos     bool
	nsHost        int
	nsBoot        string
	archiveNS     bool
	lastWitnessed int64
	warnedStuck   bool

	// zstd decompression state, set only when cfg.zstdDict is non-nil.
	zstdDictID  uint32
	zstdDecoder *zstd.Decoder
}

// LastSeq returns the highest seq the consumer has delivered, or its seeded
// dedup floor if it delivered nothing. Read it only after Run returns (the
// field is mutated on Run's goroutine); the cutover engine uses it to resume a
// re-backfill from the last durably-processed seq after a too-old 400.
func (c *liveConsumer) LastSeq() uint64 { return c.lastSeq }

func newLiveConsumer(cfg liveConfig) *liveConsumer {
	if cfg.readLimit <= 0 {
		cfg.readLimit = defaultLiveReadLimit
	}
	if cfg.dial == nil {
		hc := cfg.httpClient
		cfg.dial = func(ctx context.Context, rawURL string) (wsConn, error) {
			return dialWebsocket(ctx, rawURL, hc)
		}
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(discardWriter{}, nil))
	}
	var dictID uint32
	var dec *zstd.Decoder
	if cfg.zstdDict != nil {
		id, err := zstddict.ParseID(cfg.zstdDict)
		if err != nil {
			// The blob came from getZstdDictionary moments ago; a parse
			// failure is a server/transport fault, not a reason to crash.
			// Fall back to uncompressed (a documented degradation, logged).
			cfg.logger.Warn("invalid zstd dictionary; falling back to uncompressed live tail", "err", err)
			cfg.zstdDict = nil
		} else {
			d, derr := newZstdDecoder(cfg.zstdDict, cfg.readLimit)
			if derr != nil {
				cfg.logger.Warn("zstd decoder construction failed; falling back to uncompressed live tail", "err", derr)
				cfg.zstdDict = nil
			} else {
				dictID = id
				dec = d
			}
		}
	}
	// Seed lastSeq (the dedup floor) from dedupFloor, NOT from the wire cursor:
	// the two diverge in the empty-archive cutover, where the wire cursor is 0
	// (replay from the start) but dedupFloor is 0 meaning "nothing delivered yet"
	// so the first real event (seq >= 1) passes the dedup. A positive dedupFloor
	// (a genuine resume, or a small-tip cutover where the backfill already emitted
	// through seq) drops the at-least-once re-delivery of seq itself. seenAny
	// stays false until the first delivery so reconnect knows whether to omit the
	// wire cursor (from-tip) or resume. See liveConfig.dedupFloor.
	return &liveConsumer{
		cfg:           cfg,
		lastSeq:       cfg.dedupFloor,
		zstdDictID:    dictID,
		zstdDecoder:   dec,
		hosts:         append([]string{cfg.host}, cfg.failoverHosts...),
		hasSeqPos:     !cfg.fromTip && cfg.cursor < seqspace.CursorSeqMaxThreshold,
		archiveNS:     cfg.archiveNS,
		lastWitnessed: cfg.witnessedFloor,
	}
}

// Run tails the live stream until ctx is cancelled, invoking emit for each
// decoded event in delivery order. emit returning false stops the consumer.
// Recoverable read/dial failures trigger a reconnect with backoff (reported to
// emit as a non-nil error with a nil event so the caller can observe churn);
// a context cancellation is a clean stop and returns nil.
func (c *liveConsumer) Run(ctx context.Context, emit func(*Event, error) bool) error {
	// The decoder holds worker goroutines/buffers the zstd package requires
	// Close to release; Run is the consumer's lifetime boundary, and the
	// decoder may be swapped mid-run by refreshDict, so close whatever is
	// current at exit.
	defer func() {
		if c.zstdDecoder != nil {
			c.zstdDecoder.Close()
		}
	}()
	minB, maxB := c.cfg.minBackoff(), c.cfg.maxBackoff()
	backoff := minB
	for {
		if ctx.Err() != nil {
			return nil //nolint:nilerr // context cancellation is a clean shutdown, not an error
		}
		deliveredBefore := c.delivered
		err := c.session(ctx, emit)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // ctx cancelled mid-session: clean shutdown, the session err is incidental
		}
		if errors.Is(err, errEmitStop) {
			return nil
		}
		// A boot mismatch is a planned switch to the witnessed-time path, not a
		// failure: redial at once rather than reporting churn and backing off.
		if errors.Is(err, errBootMismatch) {
			c.cfg.logger.Info("live tail reached a different jetstream process; resuming by witnessed time",
				"host", c.hosts[c.hostIdx])
			continue
		}
		// A too-old cursor is terminal, not transient: the seq will not become
		// valid by reconnecting (the lookback floor only advances). Return it so
		// the cutover engine re-enters the backfill pagination loop from the last
		// durably-processed seq (design §14 client side) instead of churning
		// reconnects against a cursor the server will keep rejecting. This covers
		// both the terminal handoff connect and a mid-stream fell-off-live drop.
		//
		// Off the archive namespace (time mode) a re-backfill would sweep from a
		// foreign seq, so leaveTooOldNamespace switches to a witnessed-time
		// resume instead and the loop reconnects.
		tooOld := errors.Is(err, errLiveCursorTooOld)
		if tooOld && c.leaveTooOldNamespace(err) {
			err = fmt.Errorf("%w; resuming by witnessed time", err)
		} else if tooOld || errors.Is(err, errLiveInvalidRequest) {
			return err
		}
		var dialErr *liveDialError
		if errors.As(err, &dialErr) && !errors.Is(err, errLiveDictRejected) && !tooOld {
			c.rotateHost()
		}
		// A dict-rejected pre-upgrade 400 means the server rotated its
		// dictionary out from under us. Unlike a too-old
		// cursor this is recoverable in-place: refresh (or shed) the
		// dictionary before the reconnect below, rather than 400-looping
		// on an ID the server will keep refusing.
		if errors.Is(err, errLiveDictRejected) {
			c.refreshDict(ctx)
		}
		// A session that made progress (delivered new events) is healthy; reset
		// backoff so a long-lived connection that finally drops reconnects
		// promptly rather than at the accumulated max.
		if c.delivered != deliveredBefore {
			backoff = minB
		}
		// Report the disconnect and back off before reconnecting.
		if err != nil && !emit(nil, fmt.Errorf("jetstream: live tail reconnecting: %w", err)) {
			return nil
		}
		if !sleep(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff, maxB)
	}
}

// errEmitStop unwinds the session loop when the consumer asks to stop.
var errEmitStop = errors.New("jetstream: live emit stop")

// session runs one connection: dial, read-decode-emit until an error or stop.
// A successful read resets the caller's backoff via the return path (nil err).
func (c *liveConsumer) session(ctx context.Context, emit func(*Event, error) bool) error {
	rawURL, seqResume := c.planSession()
	conn, err := c.cfg.dial(ctx, rawURL)
	if err != nil {
		return &liveDialError{err: err}
	}
	conn.SetReadLimit(c.cfg.readLimit)
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "client closing") }()
	if c.cfg.timeMode {
		// Checked before the first read, so no frame from an unverified
		// namespace is ever deduped against (or delivered after) lastSeq.
		if err := c.adoptSession(connBootID(conn), seqResume); err != nil {
			return err
		}
	}

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		switch {
		case c.zstdDecoder != nil && typ == websocket.MessageBinary:
			// dict-zstd connection: every event frame is a BINARY zstd
			// frame; decompress before decode. DecodeAll enforces the
			// decoder's memory limit, bounding a hostile frame.
			data, err = c.zstdDecoder.DecodeAll(data, nil)
			if err != nil {
				// Upstream input, never crash: surface and keep the tail.
				if !emit(nil, fmt.Errorf("jetstream: zstd frame decode: %w", err)) {
					return errEmitStop
				}
				continue
			}
		case typ != websocket.MessageText:
			continue // jetstream frames are text JSON; ignore stray binary
		}
		ev, info, derr := decodeLiveFrame(data, c.cfg.mode)
		if errors.Is(derr, errSkipFrame) {
			if info != nil {
				// An #info advisory (OutdatedCursor on a clamped timestamp
				// resume, or a future advisory). Not an event: no seq, no
				// cursor advance. Operator-relevant, so log it.
				c.cfg.logger.Info("live stream info frame", "name", info.Name, "message", info.Message)
				// A clamped time resume may have skipped events; a time-mode
				// caller relies on that resume being gapless, so tell it.
				if c.cfg.timeMode && info.Name == infoNameOutdatedCursor {
					if !emit(nil, fmt.Errorf("%w: %s", ErrCursorClamped, info.Message)) {
						return errEmitStop
					}
				}
			}
			continue
		}
		var streamErr *liveStreamError
		if errors.As(derr, &streamErr) {
			// A terminal error frame: the server closes right after sending
			// it. Return it so the reconnect loop backs off and resumes —
			// the same flow as the abrupt close it replaces, but with the
			// typed reason attached for the caller's error slot.
			return streamErr
		}
		if derr != nil {
			// A malformed data frame is upstream input; surface it but keep the
			// connection (one bad frame must not drop the tail).
			if !emit(nil, derr) {
				return errEmitStop
			}
			continue
		}
		// Deduplicate the at-least-once reconnect overlap: skip anything at or
		// below the highest seq already delivered. lastSeq 0 with nothing yet
		// delivered means the first real event (seq >= 1) passes — the seq-0
		// swallow is structurally impossible under 1-based seqs.
		if ev.Seq <= c.lastSeq {
			continue
		}
		c.lastSeq = ev.Seq
		c.seenAny = true
		c.delivered++
		c.hasSeqPos = true
		if ev.WitnessedAtUS > 0 {
			c.lastWitnessed = ev.WitnessedAtUS
		}
		evCopy := ev
		if !emit(&evCopy, nil) {
			return errEmitStop
		}
	}
}

// newZstdDecoder builds a dictionary-seeded decoder for live frames. The
// decoded-size cap mirrors the connection's read limit: on an uncompressed
// connection a frame's JSON must fit readLimit on the wire, so bounding the
// decompressed output to the same value preserves that contract and stops a
// hostile frame from expanding toward the library's 64 GiB default
// (DecodeAll rejects larger output with an error, surfaced per-frame).
func newZstdDecoder(dict []byte, readLimit int64) (*zstd.Decoder, error) {
	return zstd.NewReader(nil,
		zstd.WithDecoderDicts(dict),
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(uint64(readLimit)))
}

// refreshDict recovers from a server-side dictionary rotation after a
// dict-rejected reconnect refusal: re-fetch the current dictionary and swap
// the decoder so the next dial negotiates the new ID. When the refetch is
// unavailable, fails, or returns the very ID the server just rejected (e.g.
// a mixed-version fleet behind a load balancer), degrade to an uncompressed
// tail for this consumer's lifetime — compression is an optimization; the
// tail must keep flowing.
func (c *liveConsumer) refreshDict(ctx context.Context) {
	if c.zstdDecoder == nil {
		return // not a zstd connection; nothing to refresh
	}
	rejected := c.zstdDictID
	if c.cfg.refetchDict != nil {
		if blob := c.cfg.refetchDict(ctx, c.hosts[c.hostIdx]); blob != nil {
			if id, perr := zstddict.ParseID(blob); perr == nil && id != rejected {
				if d, derr := newZstdDecoder(blob, c.cfg.readLimit); derr == nil {
					c.zstdDecoder.Close()
					c.zstdDecoder = d
					c.zstdDictID = id
					c.cfg.logger.Info("live zstd dictionary rotated; refetched",
						"rejected_id", rejected, "new_id", id)
					return
				}
			}
		}
	}
	c.zstdDecoder.Close()
	c.zstdDecoder = nil
	c.zstdDictID = 0
	c.cfg.logger.Warn("live zstd dictionary rejected and refetch unavailable; continuing uncompressed",
		"rejected_id", rejected)
}

// planSession picks the URL for the next session. seqResume reports that it
// resumes the current seq namespace, which adoptSession must then verify.
func (c *liveConsumer) planSession() (rawURL string, seqResume bool) {
	if !c.cfg.timeMode {
		return c.subscribeURL(), false
	}
	ts, omit, canLeave := c.resumeTS()
	if c.hasSeqPos && c.hostIdx != c.nsHost && !canLeave {
		// Nothing to resume from elsewhere; the seq position is all we have.
		c.hostIdx = c.nsHost
	}
	if (c.hasSeqPos && c.hostIdx == c.nsHost) || !canLeave {
		return c.subscribeURL(), true
	}
	return c.buildURL(c.hosts[c.hostIdx], ts, !omit), false
}

// resumeTS returns the wire cursor for a session that leaves the current seq
// namespace, or ok=false if there is no position to leave with (nothing
// delivered carried witnessedAt, e.g. an older server). omit means "start at
// the live tip" for a from-tip start that has delivered nothing.
func (c *liveConsumer) resumeTS() (cursor uint64, omit, ok bool) {
	switch {
	case c.lastWitnessed > 0:
		// Instances witness independently, so the same event's witnessed_at
		// differs across hosts; the rewind absorbs that skew.
		return rewindTimeCursor(uint64(c.lastWitnessed), c.cfg.rewind), false, true
	case !c.seenAny && c.cfg.fromTip:
		return 0, true, true
	case !c.seenAny && c.cfg.cursor >= seqspace.CursorSeqMaxThreshold:
		return c.cfg.cursor, false, true // already rewound by the engine
	}
	return 0, false, false
}

// adoptSession applies a connected session's boot ID. A seq resume that
// landed on a different process is abandoned (errBootMismatch) when a time
// resume is possible; a time resume starts a fresh seq namespace.
func (c *liveConsumer) adoptSession(boot string, seqResume bool) error {
	if seqResume {
		if c.nsBoot != "" && boot != c.nsBoot {
			if _, _, ok := c.resumeTS(); ok {
				c.hasSeqPos = false
				return errBootMismatch
			}
			c.warnStuck("live tail reached a different jetstream process but cannot resume by witnessed time; continuing by seq")
		}
		c.nsBoot = boot
		return nil
	}
	// The overlap from the rewind is re-delivered (at-least-once): the old
	// lastSeq is meaningless here and would drop events if this host's seqs
	// are lower.
	c.nsHost = c.hostIdx
	c.nsBoot = boot
	c.archiveNS = false
	c.lastSeq = 0
	return nil
}

// leaveTooOldNamespace reports whether a CursorTooOld should be answered by a
// witnessed-time resume rather than returned for a re-backfill. Re-backfill
// is only valid when the rejected seq is in the archive's namespace.
func (c *liveConsumer) leaveTooOldNamespace(err error) bool {
	if !c.cfg.timeMode {
		return false
	}
	if c.archiveNS && (c.nsBoot == "" || c.nsBoot == errBootID(err)) {
		return false
	}
	if _, _, ok := c.resumeTS(); !ok {
		return false
	}
	c.hasSeqPos = false
	return true
}

// rotateHost moves to the next host after a failed dial, if leaving the
// current seq namespace is possible.
func (c *liveConsumer) rotateHost() {
	if !c.cfg.timeMode || len(c.hosts) < 2 {
		return
	}
	if _, _, ok := c.resumeTS(); c.hasSeqPos && !ok {
		c.warnStuck("live tail cannot fail over: no witnessed time to resume from yet")
		return
	}
	c.hostIdx = (c.hostIdx + 1) % len(c.hosts)
}

func (c *liveConsumer) warnStuck(msg string) {
	if c.warnedStuck {
		return
	}
	c.warnedStuck = true
	c.cfg.logger.Warn(msg, "host", c.hosts[c.hostIdx])
}

// subscribeURL builds the seq-resume URL for the current host.
func (c *liveConsumer) subscribeURL() string {
	// Wire cursor: once any event has been delivered, resume each new session at
	// lastSeq (the highest seq delivered). subscribeURL is rebuilt on every
	// reconnect, so anchoring at lastSeq is what keeps a stream from re-anchoring
	// at the reconnect-time tip and silently dropping events produced while
	// disconnected. Before any delivery we use the configured start: omit the
	// param when fromTip (the WithLiveCursor(0) "live from tip" contract),
	// otherwise send cursor=cfg.cursor (cursor=0 replays from the beginning).
	host := c.hosts[c.hostIdx]
	switch {
	case c.seenAny:
		return c.buildURL(host, c.lastSeq, true)
	case !c.cfg.fromTip:
		return c.buildURL(host, c.cfg.cursor, true)
	}
	return c.buildURL(host, 0, false)
}

func (c *liveConsumer) buildURL(host string, cursor uint64, withCursor bool) string {
	u, _ := url.Parse(host) // hosts are pre-normalized by the caller
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/xrpc/" + subscribeNSID
	q := url.Values{}
	if withCursor {
		q.Set("cursor", strconv.FormatUint(cursor, 10))
	}
	// Forward the caller's filters server-side. The server reads each
	// collection/DID as its own repeated param, so append (not Set) one
	// entry per value. Empty slices add nothing, leaving an unfiltered
	// tail's URL unchanged. The client-side matcher (wantsLive) remains
	// the correctness backstop.
	for _, kind := range c.cfg.kinds {
		q.Add("kinds", string(kind))
	}
	for _, c := range c.cfg.collections {
		q.Add("collections", c)
	}
	for _, d := range c.cfg.dids {
		q.Add("dids", d)
	}
	if c.zstdDecoder != nil {
		q.Set("zstdDictionary", strconv.FormatUint(uint64(c.zstdDictID), 10))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// subscribeNSID is the lexicon NSID the live tail dials at /xrpc/<nsid>.
const subscribeNSID = "network.bsky.jetstream.subscribeEvents"

// subscribeSubprotocol is the xrpc.v1.json wire subprotocol (proposal
// 0015) offered via Sec-WebSocket-Protocol. It is also the stream's
// lexicon-declared default, so an empty server echo means identical
// framing; the offer exists so negotiation-aware servers and middleboxes
// see an explicit token. MUST equal
// api/jetstream.JetstreamSubscribeEvents_Subprotocol (the client module can't
// depend on which side generated it; the contract test pins them equal).
const subscribeSubprotocol = "xrpc.v1.json"

// liveDialOptions builds the websocket DialOptions for the live tail.
// permessage-deflate is deliberately NOT offered: the v2 endpoint never
// negotiates it (#294 removed it server-side — per-connection deflate is
// the dominant server cost at fanout scale), so offering it is dead
// weight on the handshake. Compression is the dict-zstd scheme,
// negotiated at the application layer via ?zstdDictionary=<id>.
func liveDialOptions(hc *http.Client) *websocket.DialOptions {
	return &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
		HTTPClient:      hc, // nil is fine: websocket.Dial falls back to its default
		Subprotocols:    []string{subscribeSubprotocol},
	}
}

// errLiveCursorTooOld marks a terminal connect refusal: the seq cursor
// resolved below the server's lookback floor and the server returned a
// pre-upgrade HTTP 400 CursorTooOld (subscribe §14 / design §14). It is NOT a
// transient dial failure to reconnect-loop on — the cursor will not become
// valid by retrying. The cutover engine catches it and re-enters the backfill
// pagination loop from the last durably-processed seq (design §14 client
// side). The wrapped message carries the server's floor-seq message for
// observability.
var errLiveCursorTooOld = errors.New("jetstream: live cursor too old")

// errLiveInvalidRequest marks a permanent pre-upgrade configuration rejection.
// Reconnecting with the same immutable filter cannot succeed, so it terminates
// the consumer and is surfaced as ErrFatal by the engine.
var errLiveInvalidRequest = errors.New("jetstream: live request rejected")

// errLiveDictRejected marks a pre-upgrade HTTP 400 UnknownZstdDictionary
// refusing the client's ?zstdDictionary ID: the server rotated its
// dictionary (retrain + redeploy) and no longer serves the ID this consumer
// pinned at construction. Unlike errLiveCursorTooOld it is recoverable
// in-place — the consumer refetches the current dictionary (or degrades to
// uncompressed) and reconnects; see refreshDict.
var errLiveDictRejected = errors.New("jetstream: live zstd dictionary rejected")

// XRPC error names the server uses on pre-upgrade rejections. They are the
// wire contract (declared in the network.bsky.jetstream.subscribeEvents lexicon);
// the client matches the structured envelope's error name, never body
// substrings. The internal/subscribe handler is the emitting side; the
// cross-package contract test (live_subscribe_contract_test.go) pins the
// two ends against the real handler so a drift fails CI.
const (
	infoNameOutdatedCursor = "OutdatedCursor"
	errNameCursorTooOld    = "CursorTooOld"
	errNameUnknownZstdDict = "UnknownZstdDictionary"
	errNameInvalidRequest  = "InvalidRequest"
)

// bootIDHeader names the per-process ID the server sends on every
// subscribeEvents response. It MUST equal internal/subscribe.BootIDHeader
// (the contract test pins them equal).
const bootIDHeader = "Jetstream-Boot-Id"

// errBootMismatch ends a session that resumed by seq but reached a different
// server process than the one that assigned the seq.
var errBootMismatch = errors.New("jetstream: live tail boot id changed")

// liveDialError marks a session that failed before a connection existed, the
// only failure that moves the consumer to the next failover host.
type liveDialError struct{ err error }

func (e *liveDialError) Error() string { return "dial: " + e.err.Error() }
func (e *liveDialError) Unwrap() error { return e.err }

// bootConn carries the handshake's boot ID alongside the connection, so the
// dialFunc signature (and the test fakes) stay unchanged.
type bootConn struct {
	*websocket.Conn
	boot string
}

func (b bootConn) bootID() string { return b.boot }

// bootIDError carries the boot ID of a pre-upgrade rejection.
type bootIDError struct {
	boot string
	err  error
}

func (e *bootIDError) Error() string { return e.err.Error() }
func (e *bootIDError) Unwrap() error { return e.err }

func connBootID(conn wsConn) string {
	if b, ok := conn.(interface{ bootID() string }); ok {
		return b.bootID()
	}
	return ""
}

func errBootID(err error) string {
	var be *bootIDError
	if errors.As(err, &be) {
		return be.boot
	}
	return ""
}

// dialWebsocket is the production dialer. hc, when non-nil, routes the HTTP/1.1
// upgrade through a custom transport (e.g. an in-process pipe); nil uses the
// websocket default.
//
// Pre-upgrade rejections arrive as XRPC JSON error envelopes
// ({"error": name, "message": ...}); CursorTooOld maps to the typed
// errLiveCursorTooOld (terminal) and UnknownZstdDictionary to
// errLiveDictRejected (recoverable). coder/websocket leaves the first 1024
// bytes of the response body readable on a non-101 handshake, which carries
// the envelope.
//
// On a successful handshake the server's subprotocol echo is verified:
// RFC 6455 §4.1 requires the client to fail the connection when the server
// selects a token that was not offered. An empty echo is the proposal-0015
// lexicon-default fallback and means identical framing.
func dialWebsocket(ctx context.Context, rawURL string, hc *http.Client) (wsConn, error) {
	conn, resp, err := websocket.Dial(ctx, rawURL, liveDialOptions(hc))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusBadRequest && resp.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			var envelope struct {
				Error   string `json:"error"`
				Message string `json:"message"`
			}
			if jerr := json.Unmarshal(body, &envelope); jerr == nil {
				var typed error
				switch envelope.Error {
				case errNameCursorTooOld:
					typed = errLiveCursorTooOld
				case errNameUnknownZstdDict:
					typed = errLiveDictRejected
				case errNameInvalidRequest:
					typed = errLiveInvalidRequest
				}
				if typed != nil {
					return nil, &bootIDError{
						boot: resp.Header.Get(bootIDHeader),
						err:  fmt.Errorf("%w: %s", typed, envelope.Message),
					}
				}
			}
		} else if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	var boot string
	if resp != nil {
		boot = resp.Header.Get(bootIDHeader)
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}
	if echoed := conn.Subprotocol(); echoed != "" && echoed != subscribeSubprotocol {
		_ = conn.Close(websocket.StatusProtocolError, "unoffered subprotocol")
		return nil, fmt.Errorf("jetstream: server selected unoffered subprotocol %q", echoed)
	}
	return bootConn{Conn: conn, boot: boot}, nil
}

func nextBackoff(d, maxB time.Duration) time.Duration {
	d *= 2
	if d > maxB {
		return maxB
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

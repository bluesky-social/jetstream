package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/jttp"
	"golang.org/x/sync/errgroup"
)

// Whole-segment downloads use HTTP ranges two ways (#296). Always on: a
// mid-stream failure resumes with Range+If-Range from the last byte received —
// O(gap) recovery instead of the old O(segment) restart-from-zero. Opt-in
// (WithSegmentStripes > 1): the segment is fetched as parallel range parts,
// for paths where per-stream congestion control is the throughput bound.
//
// Striping is OFF by default on measured evidence: across a WireGuard tunnel
// (Boston→Seattle, 70 ms) all streams share one encapsulated UDP flow, so
// parallel parts fragment the tunnel's capacity instead of adding any —
// 8-part striping measured 20-40% SLOWER than the single warm stream. On the
// raw internet, where routers/limiters see per-TCP-flow state, striping is
// expected to win; flip it on once that is measured, not before.
//
// The server's getSegment serves via http.ServeContent, so Range, If-Range,
// and a strong per-generation ETag are already part of the contract; the
// plan's Checksum field was reserved for exactly this.
const (
	// segmentPartSize is the striped-mode range granularity. Small enough that
	// a ~280 MB segment yields ~18 parts (keeping all stripes busy through the
	// tail), large enough that per-request overhead (RTT + TTFB) stays <1% of
	// a part's transfer time on measured WAN paths.
	segmentPartSize = 16 << 20
	// defaultSegmentStripes keeps whole-segment fetches single-stream (see the
	// package comment above for the measurement that set this).
	defaultSegmentStripes = 1
	// maxSegmentBytes bounds a single segment allocation, mirroring the old
	// xrpc.QueryRaw cap's role: a corrupt/hostile Content-Range or
	// Content-Length must not make the client allocate unbounded memory.
	// Sealed segments are ~256-280 MB.
	maxSegmentBytes = 1 << 30
	// maxGenerationAttempts bounds whole-segment restarts when a compaction
	// swaps the file's generation mid-download (detected via If-Range → 200).
	// Compactions are rare; two swaps during one segment download means
	// something is deeply wrong, so surface it as the entry's error.
	maxGenerationAttempts = 2
	// maxRetryAfterWait caps how long a server-supplied Retry-After /
	// RateLimit-Reset can defer a retry, matching xrpc's MaxDelay. Longer
	// requests fall back to exponential backoff rather than stalling the
	// download pipeline for minutes on one throttled part.
	maxRetryAfterWait = 30 * time.Second
)

// errSegmentGenerationChanged signals that the segment's ETag changed between
// range parts (compaction rewrote the file mid-download). The fetch restarts
// from the probe rather than splicing bytes from two generations.
var errSegmentGenerationChanged = errors.New("segment generation changed mid-download")

// defaultBulkHTTP is the fallback transport when the xrpc client was built
// without an explicit HTTP client (direct Downloader construction in tests);
// the engine always injects one. Built lazily with the same bulk tuning the
// engine uses so behavior matches either way.
var (
	defaultBulkHTTPOnce sync.Once
	defaultBulkHTTP     *http.Client
)

func (d *Downloader) httpClient() *http.Client {
	if d.xc.HTTPClient.HasVal() {
		return d.xc.HTTPClient.Val()
	}
	defaultBulkHTTPOnce.Do(func() {
		defaultBulkHTTP = jttp.New(xrpc.BulkDownloadOpts()...)
	})
	return defaultBulkHTTP
}

// partAttempts is the per-part attempt budget. It honors the caller's
// WithMaxDownloadAttempts (plumbed onto the xrpc client's retry policy);
// the bulk transport itself is WithNoRetries, so this loop owns all retry
// behavior on the segment path — at part granularity, which is the point.
// Clamped to ≥1 to match xrpc's own MaxAttempts semantics (0 means "one
// attempt, no retries", never "zero attempts").
func (d *Downloader) partAttempts() int {
	return max(d.xc.Retry.ValOr(xrpc.DefaultRetryPolicy).MaxAttempts.ValOr(3), 1)
}

func (d *Downloader) segmentURL(name string) string {
	return strings.TrimSuffix(d.xc.Host, "/") +
		"/xrpc/network.bsky.jetstream.getSegment?name=" + url.QueryEscape(name)
}

// fetchSegment downloads one sealed segment file, striped across parallel
// range requests when the server supports them, with a transparent
// single-stream fallback when it does not (or sends no ETag, without which
// parts from different generations could be spliced).
func (d *Downloader) fetchSegment(ctx context.Context, name string) ([]byte, error) {
	var lastErr error
	for range maxGenerationAttempts {
		buf, err := d.fetchSegmentGeneration(ctx, name)
		if err == nil {
			return buf, nil
		}
		if !errors.Is(err, errSegmentGenerationChanged) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("%w after %d attempts (compaction churn?)", lastErr, maxGenerationAttempts)
}

// fetchSegmentGeneration performs one consistent-generation download attempt.
func (d *Downloader) fetchSegmentGeneration(ctx context.Context, name string) ([]byte, error) {
	u := d.segmentURL(name)
	partSize := d.segPartSize

	// Probe: request the first part. The response tells us whether the server
	// honors ranges (206 + Content-Range + ETag → range mode) or not (200 → it
	// is already streaming the whole file; consume it as the fallback path).
	// Retried like any part: the old xrpc.QueryRaw path retried the whole
	// request, so the probe must not be a single point of transient failure.
	probe, err := d.probeWithRetry(ctx, u, partSize-1)
	if err != nil {
		return nil, fmt.Errorf("probe %q: %w", name, err)
	}
	defer func() { _ = probe.Body.Close() }()

	switch probe.StatusCode {
	case http.StatusOK:
		// Range ignored; the server is already streaming the whole file.
		// Consume it, but a mid-body failure must get the fallback's retry
		// budget like every other read path here (there is no validator, so
		// each retry restarts from byte 0, matching pre-#296 behavior).
		buf, err := readFullBody(probe, maxSegmentBytes)
		if err == nil {
			return buf, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// The failed probe read consumed one attempt; the fallback gets the
		// remainder of the budget (attempts==1 → none: fail fast).
		remaining := d.partAttempts() - 1
		if remaining <= 0 {
			return nil, fmt.Errorf("probe %q: %w", name, err)
		}
		_ = probe.Body.Close()
		return d.fetchWholeFallbackAttempts(ctx, u, remaining)
	case http.StatusPartialContent:
	default:
		return nil, httpStatusError(probe)
	}

	start, end, total, err := parseContentRange(probe.Header.Get("Content-Range"))
	if err != nil {
		return nil, fmt.Errorf("probe %q: %w", name, err)
	}
	if start != 0 {
		return nil, fmt.Errorf("probe %q: server returned range starting at %d, want 0", name, start)
	}
	if total > maxSegmentBytes {
		return nil, fmt.Errorf("segment %q: size %d exceeds the %d-byte client cap", name, total, int64(maxSegmentBytes))
	}
	etag := probe.Header.Get("ETag")
	if etag == "" {
		// No generation validator: ranged continuation could splice two
		// generations on a compaction rewrite. Fall back to one full-body
		// request (pre-#296 behavior, same safety).
		_ = probe.Body.Close()
		return d.fetchWholeFallback(ctx, u)
	}

	buf := make([]byte, total)
	if n, err := io.ReadFull(probe.Body, buf[:end-start+1]); err != nil {
		// The probe body died mid-read. Keep the bytes that arrived and let
		// fetchPart (bounded retries, If-Range validation) fill the gap — the
		// probe body must not be a single point of transient failure any more
		// than the probe request is. Matters most for segments that complete
		// entirely in the probe: they never reach the resumable paths below.
		// The failed probe read consumed one attempt; the gap-fill gets the
		// remainder of the budget (attempts==1 → none: fail fast).
		_ = probe.Body.Close()
		remaining := d.partAttempts() - 1
		if remaining <= 0 {
			return nil, fmt.Errorf("probe %q: read body: %w", name, err)
		}
		if perr := d.fetchPartAttempts(ctx, u, name, buf[n:end+1], int64(n), end, etag, remaining); perr != nil {
			return nil, fmt.Errorf("probe %q: read body: %w", name, perr)
		}
	}
	if end+1 >= total {
		return buf, nil
	}

	if d.segStripes <= 1 {
		// Single-stream mode (default): fetch the remainder as ONE ranged
		// request on the same warm connection, resuming from the last byte
		// received on transient failure — O(gap) recovery, identical wire
		// pattern to the pre-#296 path when nothing fails.
		if err := d.fetchRemainderResumable(ctx, u, name, buf, end+1, etag); err != nil {
			return nil, err
		}
		return buf, nil
	}

	// Striped mode: fan the remaining parts out across the stripe pool, each
	// reading directly into its slice of buf (disjoint ranges — no copies, no
	// locks).
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(d.segStripes)
	for off := end + 1; off < total; off += partSize {
		last := min(off+partSize-1, total-1)
		g.Go(func() error {
			return d.fetchPart(gctx, u, name, buf[off:last+1], off, last, etag)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return buf, nil
}

// fetchRemainderResumable downloads buf[from:] as a single ranged stream,
// resuming from wherever the previous attempt died. Progress made by a failed
// attempt is kept — the retry requests only the missing suffix, so N transient
// failures cost N gap-refetches, never N whole-segment restarts (#296).
//
// Budget semantics: attempts==1 (WithMaxDownloadAttempts(1), documented as
// "disables retries entirely") means exactly one request — a mid-body failure
// is not resumed. For attempts>1, a failed attempt only refunds the budget
// when it delivered meaningful progress (≥ min(segPartSize, what was left)),
// so a long transfer with occasional hiccups completes, but a hostile server
// dribbling a byte per connection cannot force unbounded requests: worst case
// is attempts requests per partSize of the segment.
func (d *Downloader) fetchRemainderResumable(ctx context.Context, u, name string, buf []byte, from int64, etag string) error {
	total := int64(len(buf))
	attempts := d.partAttempts()
	remaining := attempts
	var lastErr error
	for off := from; off < total; {
		if remaining <= 0 {
			return fmt.Errorf("segment %q: resume stalled at byte %d/%d: %w (after %d attempts without meaningful progress)",
				name, off, total, lastErr, attempts)
		}
		if lastErr != nil {
			// Backoff grows with consecutive unrefunded failures (attempt 1 after
			// the first; max() guards the progress-refund case where remaining
			// was just restored to the full budget). A server-supplied
			// Retry-After on the previous error extends the wait.
			delay := retryBackoff(d.partRetryDelay, max(attempts-remaining, 1), lastErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		want := total - off
		n, err := d.readRangeInto(ctx, u, buf[off:], off, total-1, etag)
		off += n
		if err == nil {
			return nil
		}
		if errors.Is(err, errSegmentGenerationChanged) || ctx.Err() != nil {
			return err
		}
		lastErr = err
		if attempts > 1 && n >= min(d.segPartSize, want) {
			remaining = attempts // meaningful progress: refund the budget
		} else {
			remaining--
		}
	}
	return nil
}

// readRangeInto issues one ranged request for buf-window [off, last] and reads
// as many bytes as arrive before an error, returning the byte count so the
// caller can resume from the exact failure point.
func (d *Downloader) readRangeInto(ctx context.Context, u string, dst []byte, off, last int64, etag string) (int64, error) {
	resp, err := d.doRangeRequest(ctx, u, off, last, etag)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// A missing ETag is treated the same as a mismatch: range mode was
		// chosen because the probe carried one, so a continuation without it
		// cannot prove it's the same generation — and unverifiable bytes must
		// never be spliced into the buffer.
		if got := resp.Header.Get("ETag"); got != etag {
			return 0, errSegmentGenerationChanged
		}
		start, end, _, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return 0, err
		}
		if start != off || end != last {
			return 0, fmt.Errorf("server returned range %d-%d, want %d-%d", start, end, off, last)
		}
		n, err := io.ReadFull(resp.Body, dst)
		if err != nil {
			return int64(n), fmt.Errorf("read range body at %d: %w", off+int64(n), err)
		}
		return int64(n), nil
	case http.StatusOK, http.StatusRequestedRangeNotSatisfiable:
		return 0, errSegmentGenerationChanged
	default:
		return 0, httpStatusError(resp)
	}
}

// fetchPart downloads one range part with bounded retries. Transient failures
// (network errors, 5xx, short reads) retry with backoff; a generation change
// or context cancellation aborts immediately.
func (d *Downloader) fetchPart(ctx context.Context, u, name string, dst []byte, off, last int64, etag string) error {
	return d.fetchPartAttempts(ctx, u, name, dst, off, last, etag, d.partAttempts())
}

// fetchPartAttempts is fetchPart with an explicit attempt budget, for callers
// that already spent part of theirs (the probe body gap-fill).
func (d *Downloader) fetchPartAttempts(ctx context.Context, u, name string, dst []byte, off, last int64, etag string, attempts int) error {
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			delay := retryBackoff(d.partRetryDelay, attempt, lastErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		err := d.tryPart(ctx, u, dst, off, last, etag)
		if err == nil {
			return nil
		}
		if errors.Is(err, errSegmentGenerationChanged) || ctx.Err() != nil {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("segment %q part %d-%d: %w (after %d attempts)", name, off, last, lastErr, attempts)
}

func (d *Downloader) tryPart(ctx context.Context, u string, dst []byte, off, last int64, etag string) error {
	resp, err := d.doRangeRequest(ctx, u, off, last, etag)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// Missing ETag == mismatch; see readRangeInto.
		if got := resp.Header.Get("ETag"); got != etag {
			return errSegmentGenerationChanged
		}
		start, end, _, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return err
		}
		if start != off || end != last {
			return fmt.Errorf("server returned range %d-%d, want %d-%d", start, end, off, last)
		}
		if _, err := io.ReadFull(resp.Body, dst); err != nil {
			return fmt.Errorf("read part body: %w", err)
		}
		return nil
	case http.StatusOK, http.StatusRequestedRangeNotSatisfiable:
		// 200: If-Range validator mismatched (or the server stopped honoring
		// ranges); 416: the file shrank. Either way this generation is gone.
		return errSegmentGenerationChanged
	default:
		return httpStatusError(resp)
	}
}

// fetchWholeFallback is the plain full-body download (no Range),
// byte-equivalent to the pre-striping behavior — including its retries: the
// bulk transport is WithNoRetries, so this loop is the only retry mechanism
// on the path taken when the server ignores Range or omits an ETag. Transient
// transport errors, 5xx, and short body reads retry with the same budget and
// backoff as every other path here; there is no resume (no validator), so
// each attempt restarts from byte 0 like the pre-#296 client did.
func (d *Downloader) fetchWholeFallback(ctx context.Context, u string) ([]byte, error) {
	return d.fetchWholeFallbackAttempts(ctx, u, d.partAttempts())
}

// fetchWholeFallbackAttempts is fetchWholeFallback with an explicit attempt
// budget, for callers that already spent part of theirs (a failed 200 probe).
func (d *Downloader) fetchWholeFallbackAttempts(ctx context.Context, u string, attempts int) ([]byte, error) {
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			delay := retryBackoff(d.partRetryDelay, attempt, lastErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		buf, err := d.tryWhole(ctx, u)
		if err == nil {
			return buf, nil
		}
		var permanent *permanentError
		if errors.As(err, &permanent) || ctx.Err() != nil {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("%w (after %d attempts)", lastErr, attempts)
}

func (d *Downloader) tryWhole(ctx context.Context, u string) ([]byte, error) {
	req, err := d.newSegmentRequest(ctx, u)
	if err != nil {
		return nil, err
	}
	resp, err := d.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		err := httpStatusError(resp)
		if !retryableStatus(resp.StatusCode) {
			return nil, &permanentError{err: err}
		}
		return nil, err
	}
	return readFullBody(resp, maxSegmentBytes)
}

// permanentError marks a failure that retrying cannot fix (e.g. a 4xx XRPC
// error envelope); fetchWholeFallback surfaces it immediately.
type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// retryableStatus mirrors the xrpc retry policy's status classification
// (isRetryable): 5xx server faults plus 429 throttling, which the pre-#296
// QueryRaw path retried and which getBlock requests still retry.
func retryableStatus(code int) bool {
	return code >= 500 || code == http.StatusTooManyRequests
}

// probeWithRetry issues the opening range request with the same attempt budget
// as a part. Responses with retryable statuses (5xx, 429) are drained and
// retried; any other 2xx/3xx/4xx response is returned for interpretation.
func (d *Downloader) probeWithRetry(ctx context.Context, u string, lastByte int64) (*http.Response, error) {
	attempts := d.partAttempts()
	var lastErr error
	for attempt := range attempts {
		if attempt > 0 {
			delay := retryBackoff(d.partRetryDelay, attempt, lastErr)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		resp, err := d.doRangeRequest(ctx, u, 0, lastByte, "")
		if err == nil && !retryableStatus(resp.StatusCode) {
			return resp, nil
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			lastErr = err
			continue
		}
		lastErr = httpStatusError(resp)
		_ = resp.Body.Close()
	}
	return nil, fmt.Errorf("%w (after %d attempts)", lastErr, attempts)
}

func (d *Downloader) doRangeRequest(ctx context.Context, u string, off, last int64, etag string) (*http.Response, error) {
	req, err := d.newSegmentRequest(ctx, u)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, last))
	if etag != "" {
		req.Header.Set("If-Range", etag)
	}
	return d.httpClient().Do(req)
}

// newSegmentRequest builds a segment GET with the same identity headers the
// xrpc QueryRaw path sets (User-Agent, Accept, Authorization when the client
// has a session) — the raw range path must not silently drop auth that
// getBlock requests still carry.
func (d *Downloader) newSegmentRequest(ctx context.Context, u string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", d.xc.UserAgent.ValOr("go/atmos"))
	req.Header.Set("Accept", "*/*")
	if auth := d.xc.Auth(); auth != nil && auth.AccessJwt != "" {
		req.Header.Set("Authorization", "Bearer "+auth.AccessJwt)
	}
	return req, nil
}

// readFullBody reads a non-range response body, pre-sizing from Content-Length
// when available and enforcing cap either way.
func readFullBody(resp *http.Response, limit int64) ([]byte, error) {
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("response size %d exceeds the %d-byte client limit", resp.ContentLength, limit)
	}
	if resp.ContentLength >= 0 {
		buf := make([]byte, resp.ContentLength)
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
		return buf, nil
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("response exceeds the %d-byte client limit", limit)
	}
	return buf, nil
}

// statusError is a non-2xx response, carrying any server-supplied retry
// deferral (Retry-After / RateLimit-Reset) so the retry loops can honor it —
// the old xrpc.QueryRaw path did, and a throttling CDN's 429 with
// "Retry-After: 5" must not be retried after 500ms and fail the segment.
// It wraps a *xrpc.Error so errors.As keeps working on the public error
// surface, matching the pre-#296 JetstreamGetSegment behavior.
type statusError struct {
	err        *xrpc.Error
	retryAfter time.Duration // 0 = none supplied
}

func (e *statusError) Error() string { return e.err.Error() }
func (e *statusError) Unwrap() error { return e.err }

// httpStatusError drains a bounded error-body excerpt so XRPC error envelopes
// (e.g. SegmentNotFound) stay diagnosable without trusting the body length.
func httpStatusError(resp *http.Response) error {
	excerpt, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	xerr := &xrpc.Error{StatusCode: resp.StatusCode}
	var envelope struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if json.Unmarshal(excerpt, &envelope) == nil && envelope.Error != "" {
		// XRPC envelope: mirror xrpc.parseError exactly (Message may be empty).
		xerr.Name = envelope.Error
		xerr.Message = envelope.Message
	} else {
		xerr.Message = strings.TrimSpace(string(excerpt))
	}
	retryAfter := parseRetryAfter(resp.Header)
	// Parity with xrpc.parseError's metadata: rate-limit info (so callers'
	// IsRateLimited/backoff logic keeps working) and post-redirect host
	// attribution.
	if retryAfter > 0 {
		xerr.RateLimit = &xrpc.RateLimit{Reset: time.Now().Add(retryAfter)}
	}
	if resp.Request != nil && resp.Request.URL != nil {
		xerr.Host = resp.Request.URL.Host
	}
	return &statusError{err: xerr, retryAfter: retryAfter}
}

// parseRetryAfter extracts a retry deferral from RateLimit-Reset (unix
// seconds, per the atproto convention) or the standard Retry-After header
// (delta-seconds or IMF-fixdate), clamped to [0, maxRetryAfterWait]. Returns 0
// when absent or malformed — untrusted input must not stall the client.
func parseRetryAfter(h http.Header) time.Duration {
	var until time.Duration
	if reset := h.Get("RateLimit-Reset"); reset != "" {
		if unix, err := strconv.ParseInt(reset, 10, 64); err == nil {
			until = time.Until(time.Unix(unix, 0))
		}
	}
	if until == 0 {
		if ra := strings.TrimSpace(h.Get("Retry-After")); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil {
				until = time.Duration(secs) * time.Second
			} else if t, err := http.ParseTime(ra); err == nil {
				until = time.Until(t)
			}
		}
	}
	return min(max(until, 0), maxRetryAfterWait)
}

// retryBackoff computes the delay before retry `attempt` (1-based): a
// server-supplied deferral on the previous error wins over the fixed
// exponential schedule when it asks for a longer wait. The schedule is
// capped at maxRetryAfterWait (mirroring xrpc's MaxDelay), which also keeps
// the shift from overflowing under large WithMaxDownloadAttempts values.
func retryBackoff(base time.Duration, attempt int, lastErr error) time.Duration {
	delay := maxRetryAfterWait
	// Overflow-safe: base<<shift < cap  ⇔  base < cap>>shift (both positive).
	if shift := attempt - 1; shift < 63 && base < maxRetryAfterWait>>shift {
		delay = base << shift
	}
	var se *statusError
	if errors.As(lastErr, &se) && se.retryAfter > delay {
		return se.retryAfter
	}
	return delay
}

// parseContentRange parses a "bytes start-end/total" header. "*" totals (or
// any malformation) are rejected: the striped path needs an exact size.
func parseContentRange(v string) (start, end, total int64, err error) {
	rest, ok := strings.CutPrefix(v, "bytes ")
	if !ok {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q", v)
	}
	rangePart, totalStr, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q", v)
	}
	startStr, endStr, ok := strings.Cut(rangePart, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q", v)
	}
	if start, err = strconv.ParseInt(startStr, 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q: %w", v, err)
	}
	if end, err = strconv.ParseInt(endStr, 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q: %w", v, err)
	}
	if total, err = strconv.ParseInt(totalStr, 10, 64); err != nil {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q: %w", v, err)
	}
	if start < 0 || end < start || total <= end {
		return 0, 0, 0, fmt.Errorf("inconsistent Content-Range %q", v)
	}
	return start, end, total, nil
}

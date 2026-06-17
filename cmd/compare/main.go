// Command compare checks Jetstream v2 /subscribe payload compatibility against
// Jetstream v1 with a warmup/sample/drain capture model. It is a temporary
// black-box diagnostic tool and is intentionally not wired into the main app.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/urfave/cli/v3"
)

func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "jetstream-compare:", err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	return &cli.Command{
		Name:  "jetstream-compare",
		Usage: "Compare Jetstream v2 and v1 websocket payloads over an order-tolerant live sample",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "v2-url",
				Usage: "Jetstream v2 /subscribe websocket URL",
				Value: "ws://localhost:8080/subscribe",
			},
			&cli.StringFlag{
				Name:  "v1-url",
				Usage: "Jetstream v1 /subscribe websocket URL",
				Value: "wss://jetstream1.us-east.bsky.network/subscribe",
			},
			&cli.DurationFlag{
				Name:  "duration",
				Usage: "Timed sample duration after warmup overlap is reached",
				Value: time.Minute,
			},
			&cli.DurationFlag{
				Name:  "grace",
				Usage: "Drain duration after the sample window for delayed counterparts",
				Value: 10 * time.Second,
			},
			&cli.DurationFlag{
				Name:  "warmup-timeout",
				Usage: "Maximum time to wait for --min-overlap shared keys before sampling",
				Value: 5 * time.Minute,
			},
			&cli.DurationFlag{
				Name:  "dial-timeout",
				Usage: "Per-websocket dial timeout",
				Value: 10 * time.Second,
			},
			&cli.IntFlag{
				Name:  "min-overlap",
				Usage: "Shared keyed commit events required during warmup before sampling starts",
				Value: 100,
			},
			&cli.IntFlag{
				Name:  "min-sample",
				Usage: "Minimum sample keys required for a valid comparison",
				Value: 100,
			},
			&cli.IntFlag{
				Name:  "read-limit",
				Usage: "Maximum websocket message size accepted by the client",
				Value: 10_000_000,
			},
			&cli.IntFlag{
				Name:  "max-tracked-events",
				Usage: "Maximum unique keyed events to retain across warmup, sample, and grace",
				Value: 1_000_000,
			},
			&cli.IntFlag{
				Name:  "max-examples",
				Usage: "Maximum missing or mismatched examples to print per category",
				Value: 10,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg := compareConfig{
				v1URL:            cmd.String("v1-url"),
				v2URL:            cmd.String("v2-url"),
				duration:         cmd.Duration("duration"),
				grace:            cmd.Duration("grace"),
				warmupTimeout:    cmd.Duration("warmup-timeout"),
				dialTimeout:      cmd.Duration("dial-timeout"),
				minOverlap:       cmd.Int("min-overlap"),
				minSample:        cmd.Int("min-sample"),
				readLimit:        int64(cmd.Int("read-limit")),
				maxTrackedEvents: cmd.Int("max-tracked-events"),
				maxExamples:      cmd.Int("max-examples"),
				out:              cmd.Root().Writer,
			}
			if cfg.out == nil {
				cfg.out = os.Stdout
			}
			if err := cfg.validate(); err != nil {
				return err
			}

			sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
			defer stop()

			return runCompare(sigCtx, cfg)
		},
	}
}

type compareConfig struct {
	v1URL            string
	v2URL            string
	duration         time.Duration
	grace            time.Duration
	warmupTimeout    time.Duration
	dialTimeout      time.Duration
	minOverlap       int
	minSample        int
	readLimit        int64
	maxTrackedEvents int
	maxExamples      int
	out              io.Writer
}

func (c compareConfig) validate() error {
	if strings.TrimSpace(c.v1URL) == "" {
		return fmt.Errorf("v1-url is required")
	}
	if strings.TrimSpace(c.v2URL) == "" {
		return fmt.Errorf("v2-url is required")
	}
	if c.duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if c.grace < 0 {
		return fmt.Errorf("grace must be >= 0")
	}
	if c.warmupTimeout <= 0 {
		return fmt.Errorf("warmup-timeout must be > 0")
	}
	if c.dialTimeout <= 0 {
		return fmt.Errorf("dial-timeout must be > 0")
	}
	if c.minOverlap <= 0 {
		return fmt.Errorf("min-overlap must be > 0")
	}
	if c.minSample < 0 {
		return fmt.Errorf("min-sample must be >= 0")
	}
	if c.readLimit <= 0 {
		return fmt.Errorf("read-limit must be > 0")
	}
	if c.maxTrackedEvents <= 0 {
		return fmt.Errorf("max-tracked-events must be > 0")
	}
	if c.maxExamples < 0 {
		return fmt.Errorf("max-examples must be >= 0")
	}
	return nil
}

type source string

const (
	sourceV1 source = "v1"
	sourceV2 source = "v2"
)

type samplePhase int

const (
	phaseWarmup samplePhase = iota
	phaseSample
	phaseDrain
)

type eventKey struct {
	DID        string
	Rev        string
	Operation  string
	Collection string
	RKey       string
}

func (k eventKey) String() string {
	return fmt.Sprintf("%s@%s %s %s/%s", k.DID, k.Rev, k.Operation, k.Collection, k.RKey)
}

type observation struct {
	source     source
	key        eventKey
	normalized []byte
	seenAt     time.Time
	keyed      bool
}

type capturedEvent struct {
	normalized []byte
	seenAt     time.Time
}

type eventPresence struct {
	v1 *capturedEvent
	v2 *capturedEvent
}

type comparator struct {
	minOverlap int

	events        map[eventKey]*eventPresence
	sample        map[eventKey]struct{}
	warmupOverlap int

	sampleSeenV1 int
	sampleSeenV2 int
	duplicatesV1 int
	duplicatesV2 int
}

func newComparator(minOverlap int) *comparator {
	return &comparator{
		minOverlap: minOverlap,
		events:     make(map[eventKey]*eventPresence),
		sample:     make(map[eventKey]struct{}),
	}
}

func (c *comparator) observe(phase samplePhase, obs observation) {
	if !obs.keyed {
		return
	}

	presence := c.events[obs.key]
	if presence == nil {
		presence = &eventPresence{}
		c.events[obs.key] = presence
	}

	hadV1 := presence.v1 != nil
	hadV2 := presence.v2 != nil
	stored := &capturedEvent{
		normalized: append([]byte(nil), obs.normalized...),
		seenAt:     obs.seenAt,
	}

	switch obs.source {
	case sourceV1:
		if hadV1 {
			c.duplicatesV1++
			return
		}
		presence.v1 = stored
		if phase == phaseSample {
			c.sampleSeenV1++
			c.sample[obs.key] = struct{}{}
		}
	case sourceV2:
		if hadV2 {
			c.duplicatesV2++
			return
		}
		presence.v2 = stored
		if phase == phaseSample {
			c.sampleSeenV2++
			c.sample[obs.key] = struct{}{}
		}
	}

	if phase == phaseWarmup && !(hadV1 && hadV2) && presence.v1 != nil && presence.v2 != nil {
		c.warmupOverlap++
	}
}

func (c *comparator) trackedEvents() int {
	return len(c.events)
}

type missingEvent struct {
	key        eventKey
	normalized []byte
}

type payloadMismatch struct {
	key          eventKey
	v1Normalized []byte
	v2Normalized []byte
}

type lagSummary struct {
	v1First      int
	v2First      int
	equal        int
	sumAbs       time.Duration
	maxAbs       time.Duration
	matchedCount int
}

func (s lagSummary) avgAbs() time.Duration {
	if s.matchedCount == 0 {
		return 0
	}
	return s.sumAbs / time.Duration(s.matchedCount)
}

type comparisonResult struct {
	sampleKeys         int
	sampleSeenV1       int
	sampleSeenV2       int
	common             int
	onlyV1Count        int
	onlyV2Count        int
	mismatchCount      int
	onlyV1             []missingEvent
	onlyV2             []missingEvent
	mismatches         []payloadMismatch
	duplicatesV1       int
	duplicatesV2       int
	insufficientSample bool
	lag                lagSummary
}

func (r comparisonResult) failed() bool {
	return r.insufficientSample ||
		r.onlyV1Count > 0 ||
		r.onlyV2Count > 0 ||
		r.mismatchCount > 0
}

func (c *comparator) compare(maxExamples int, minSample int) comparisonResult {
	result := comparisonResult{
		sampleKeys:   len(c.sample),
		sampleSeenV1: c.sampleSeenV1,
		sampleSeenV2: c.sampleSeenV2,
		duplicatesV1: c.duplicatesV1,
		duplicatesV2: c.duplicatesV2,
	}
	result.insufficientSample = result.sampleKeys < minSample

	for _, key := range sortedSampleKeys(c.sample) {
		presence := c.events[key]
		if presence == nil {
			continue
		}
		switch {
		case presence.v1 != nil && presence.v2 != nil:
			result.common++
			result.lag.add(presence.v1.seenAt, presence.v2.seenAt)
			if !bytes.Equal(presence.v1.normalized, presence.v2.normalized) {
				result.mismatchCount++
				if len(result.mismatches) < maxExamples {
					result.mismatches = append(result.mismatches, payloadMismatch{
						key:          key,
						v1Normalized: append([]byte(nil), presence.v1.normalized...),
						v2Normalized: append([]byte(nil), presence.v2.normalized...),
					})
				}
			}
		case presence.v1 != nil:
			result.onlyV1Count++
			if len(result.onlyV1) < maxExamples {
				result.onlyV1 = append(result.onlyV1, missingEvent{
					key:        key,
					normalized: append([]byte(nil), presence.v1.normalized...),
				})
			}
		case presence.v2 != nil:
			result.onlyV2Count++
			if len(result.onlyV2) < maxExamples {
				result.onlyV2 = append(result.onlyV2, missingEvent{
					key:        key,
					normalized: append([]byte(nil), presence.v2.normalized...),
				})
			}
		}
	}

	return result
}

func (s *lagSummary) add(v1, v2 time.Time) {
	s.matchedCount++
	switch {
	case v1.Before(v2):
		s.v1First++
	case v2.Before(v1):
		s.v2First++
	default:
		s.equal++
	}

	abs := v1.Sub(v2)
	if abs < 0 {
		abs = -abs
	}
	s.sumAbs += abs
	if abs > s.maxAbs {
		s.maxAbs = abs
	}
}

func runCompare(ctx context.Context, cfg compareConfig) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	events := make(chan observation, 4096)
	errs := make(chan error, 2)

	go readStream(ctx, sourceV1, cfg.v1URL, cfg.dialTimeout, cfg.readLimit, events, errs)
	go readStream(ctx, sourceV2, cfg.v2URL, cfg.dialTimeout, cfg.readLimit, events, errs)

	comp := newComparator(cfg.minOverlap)
	fmt.Fprintf(cfg.out, "connecting v1=%s v2=%s\n", cfg.v1URL, cfg.v2URL)
	fmt.Fprintf(cfg.out, "warming up until %d shared keyed commit events overlap\n", cfg.minOverlap)

	if err := runWarmup(ctx, cfg, comp, events, errs); err != nil {
		return err
	}
	fmt.Fprintf(cfg.out, "warmup overlap reached: %d shared keys; sampling for %s\n", comp.warmupOverlap, cfg.duration)

	if err := runTimedPhase(ctx, cfg, comp, phaseSample, cfg.duration, events, errs); err != nil {
		return err
	}
	fmt.Fprintf(cfg.out, "sample complete: draining for %s\n", cfg.grace)

	if cfg.grace > 0 {
		if err := runTimedPhase(ctx, cfg, comp, phaseDrain, cfg.grace, events, errs); err != nil {
			return err
		}
	}

	result := comp.compare(cfg.maxExamples, cfg.minSample)
	printReport(cfg.out, comp, result, cfg)
	if result.failed() {
		return fmt.Errorf("streams differed")
	}
	return nil
}

func runWarmup(
	ctx context.Context,
	cfg compareConfig,
	comp *comparator,
	events <-chan observation,
	errs <-chan error,
) error {
	timer := time.NewTimer(cfg.warmupTimeout)
	defer timer.Stop()

	for comp.warmupOverlap < cfg.minOverlap {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("warmup timeout after %s: overlap=%d min=%d", cfg.warmupTimeout, comp.warmupOverlap, cfg.minOverlap)
		case err := <-errs:
			return err
		case obs := <-events:
			comp.observe(phaseWarmup, obs)
			if comp.trackedEvents() > cfg.maxTrackedEvents {
				return fmt.Errorf("exceeded max-tracked-events=%d", cfg.maxTrackedEvents)
			}
		}
	}
	return nil
}

func runTimedPhase(
	ctx context.Context,
	cfg compareConfig,
	comp *comparator,
	phase samplePhase,
	duration time.Duration,
	events <-chan observation,
	errs <-chan error,
) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case err := <-errs:
			return err
		case obs := <-events:
			comp.observe(phase, obs)
			if comp.trackedEvents() > cfg.maxTrackedEvents {
				return fmt.Errorf("exceeded max-tracked-events=%d", cfg.maxTrackedEvents)
			}
		}
	}
}

func readStream(
	ctx context.Context,
	src source,
	wsURL string,
	dialTimeout time.Duration,
	readLimit int64,
	out chan<- observation,
	errs chan<- error,
) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, resp, err := websocket.Dial(dialCtx, wsURL, nil)
	cancel()
	if err != nil {
		sendErr(ctx, errs, fmt.Errorf("%s dial %s: %w%s", src, wsURL, err, responseSuffix(resp)))
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(readLimit)

	for {
		typ, raw, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			sendErr(ctx, errs, fmt.Errorf("%s read: %w", src, err))
			return
		}
		if typ != websocket.MessageText {
			sendErr(ctx, errs, fmt.Errorf("%s unexpected websocket message type %s", src, typ))
			return
		}

		obs, err := decodeObservation(src, raw, time.Now())
		if err != nil {
			sendErr(ctx, errs, fmt.Errorf("%s decode JSON: %w", src, err))
			return
		}
		if !obs.keyed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case out <- obs:
		}
	}
}

func sendErr(ctx context.Context, errs chan<- error, err error) {
	select {
	case <-ctx.Done():
	case errs <- err:
	}
}

func responseSuffix(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	return fmt.Sprintf(" (http %d)", resp.StatusCode)
}

func decodeObservation(src source, raw []byte, seenAt time.Time) (observation, error) {
	var evt struct {
		DID    string `json:"did"`
		Kind   string `json:"kind"`
		Commit *struct {
			Rev        string `json:"rev"`
			Operation  string `json:"operation"`
			Collection string `json:"collection"`
			RKey       string `json:"rkey"`
		} `json:"commit"`
	}
	if err := json.Unmarshal(raw, &evt); err != nil {
		return observation{}, err
	}
	if evt.Kind != "commit" || evt.DID == "" || evt.Commit == nil {
		return observation{source: src, seenAt: seenAt}, nil
	}
	if evt.Commit.Rev == "" || evt.Commit.Operation == "" || evt.Commit.Collection == "" || evt.Commit.RKey == "" {
		return observation{source: src, seenAt: seenAt}, nil
	}

	normalized, err := normalizePayload(raw)
	if err != nil {
		return observation{}, err
	}
	return observation{
		source: src,
		key: eventKey{
			DID:        evt.DID,
			Rev:        evt.Commit.Rev,
			Operation:  evt.Commit.Operation,
			Collection: evt.Commit.Collection,
			RKey:       evt.Commit.RKey,
		},
		normalized: normalized,
		seenAt:     seenAt,
		keyed:      true,
	}, nil
}

func normalizePayload(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	delete(payload, "time_us")
	delete(payload, "cursor")
	return json.Marshal(payload)
}

func sortedSampleKeys(sample map[eventKey]struct{}) []eventKey {
	keys := make([]eventKey, 0, len(sample))
	for key := range sample {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return compareKeys(keys[i], keys[j]) < 0
	})
	return keys
}

func compareKeys(a, b eventKey) int {
	switch {
	case a.DID != b.DID:
		return strings.Compare(a.DID, b.DID)
	case a.Rev != b.Rev:
		return strings.Compare(a.Rev, b.Rev)
	case a.Operation != b.Operation:
		return strings.Compare(a.Operation, b.Operation)
	case a.Collection != b.Collection:
		return strings.Compare(a.Collection, b.Collection)
	default:
		return strings.Compare(a.RKey, b.RKey)
	}
}

func printReport(out io.Writer, comp *comparator, result comparisonResult, cfg compareConfig) {
	fmt.Fprintf(out, "\ncomparison sample: duration=%s grace=%s\n", cfg.duration, cfg.grace)
	fmt.Fprintf(out, "warmup_overlap=%d tracked_keys=%d\n", comp.warmupOverlap, comp.trackedEvents())
	fmt.Fprintf(out, "sample: keys=%d v1_seen=%d v2_seen=%d min_sample=%d\n",
		result.sampleKeys, result.sampleSeenV1, result.sampleSeenV2, cfg.minSample)
	fmt.Fprintf(out, "common=%d missing_from_v1=%d missing_from_v2=%d payload_mismatches=%d\n",
		result.common, result.onlyV2Count, result.onlyV1Count, result.mismatchCount)
	fmt.Fprintf(out, "duplicates: v1=%d v2=%d\n", result.duplicatesV1, result.duplicatesV2)
	fmt.Fprintf(out, "lag: v1_first=%d v2_first=%d equal=%d avg_abs=%s max_abs=%s\n",
		result.lag.v1First, result.lag.v2First, result.lag.equal, result.lag.avgAbs(), result.lag.maxAbs)
	if result.insufficientSample {
		fmt.Fprintf(out, "insufficient sample: got %d keys, need %d\n", result.sampleKeys, cfg.minSample)
	}

	printMissingExamples(out, "present only on v1", result.onlyV1)
	printMissingExamples(out, "present only on v2", result.onlyV2)
	for _, mismatch := range result.mismatches {
		fmt.Fprintf(out, "mismatch %s first_diff=%d\n", mismatch.key, firstDiff(mismatch.v1Normalized, mismatch.v2Normalized))
		fmt.Fprintf(out, "  v1 normalized: %s\n", snippet(mismatch.v1Normalized, 320))
		fmt.Fprintf(out, "  v2 normalized: %s\n", snippet(mismatch.v2Normalized, 320))
	}

	if result.failed() {
		fmt.Fprintln(out, "result: FAIL")
	} else {
		fmt.Fprintln(out, "result: PASS")
	}
}

func printMissingExamples(out io.Writer, label string, events []missingEvent) {
	for _, event := range events {
		fmt.Fprintf(out, "%s: %s\n", label, event.key)
		fmt.Fprintf(out, "  payload: %s\n", snippet(event.normalized, 320))
	}
}

func firstDiff(a []byte, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

func snippet(b []byte, limit int) string {
	if len(b) <= limit {
		return string(b)
	}
	return string(b[:limit]) + "...<truncated>"
}

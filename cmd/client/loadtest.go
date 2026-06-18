package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/format"
	"github.com/coder/websocket"
	"github.com/urfave/cli/v3"
)

// loadtestCommand is the legacy direct-websocket load tester: it opens many
// raw /subscribe (or /subscribe-v2) connections and prints throughput stats.
// It does NOT use the jetstream client library; it exists to stress the server
// websocket path directly.
func loadtestCommand() *cli.Command {
	return &cli.Command{
		Name:  "loadtest",
		Usage: "Open many raw websocket subscribers against a jetstream server and print load stats",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "url",
				Usage: "Websocket URL, HTTP(S) URL, or host[:port] for the jetstream /subscribe endpoint",
				Value: "ws://localhost:8080/subscribe",
			},
			&cli.IntFlag{
				Name:    "concurrency",
				Aliases: []string{"c"},
				Usage:   "Number of concurrent websocket subscribers",
				Value:   1,
			},
			&cli.DurationFlag{
				Name:  "report-interval",
				Usage: "How often to print summary statistics",
				Value: 5 * time.Second,
			},
			&cli.DurationFlag{
				Name:  "ramp-duration",
				Usage: "Time over which subscribers are started",
				Value: 10 * time.Second,
			},
			&cli.DurationFlag{
				Name:  "duration",
				Usage: "Optional total run duration; 0 runs until interrupted",
				Value: 0,
			},
			&cli.DurationFlag{
				Name:  "dial-timeout",
				Usage: "Per-attempt websocket dial timeout",
				Value: 10 * time.Second,
			},
			&cli.DurationFlag{
				Name:  "reconnect-delay",
				Usage: "Base delay before a failed subscriber reconnects",
				Value: time.Second,
			},
			&cli.BoolFlag{
				Name:  "compression",
				Usage: "Negotiate RFC 7692 permessage-deflate with the server",
			},
			&cli.StringFlag{
				Name:  "cursor",
				Usage: "Optional cursor query parameter",
			},
			&cli.StringSliceFlag{
				Name:  "wanted-collection",
				Usage: "Collection filter to add as wantedCollections; may be repeated",
			},
			&cli.StringSliceFlag{
				Name:  "wanted-did",
				Usage: "DID filter to add as wantedDids; may be repeated",
			},
			&cli.IntFlag{
				Name:  "max-message-size",
				Usage: "Optional maxMessageSizeBytes query parameter and hello payload field",
			},
			&cli.BoolFlag{
				Name:  "require-hello",
				Usage: "Set requireHello=true and send one initial options_update frame after dialing",
			},
			&cli.IntFlag{
				Name:  "read-limit",
				Usage: "Maximum websocket message size accepted by the client",
				Value: 10_000_000,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			cfg := config{
				rawURL:            cmd.String("url"),
				concurrency:       cmd.Int("concurrency"),
				reportInterval:    cmd.Duration("report-interval"),
				rampDuration:      cmd.Duration("ramp-duration"),
				duration:          cmd.Duration("duration"),
				dialTimeout:       cmd.Duration("dial-timeout"),
				reconnectDelay:    cmd.Duration("reconnect-delay"),
				compression:       cmd.Bool("compression"),
				cursor:            cmd.String("cursor"),
				wantedCollections: cmd.StringSlice("wanted-collection"),
				wantedDIDs:        cmd.StringSlice("wanted-did"),
				maxMessageSize:    cmd.Int("max-message-size"),
				requireHello:      cmd.Bool("require-hello"),
				readLimit:         int64(cmd.Int("read-limit")),
				out:               cmd.Root().Writer,
			}
			if cfg.out == nil {
				cfg.out = os.Stdout
			}
			if err := cfg.validate(); err != nil {
				return err
			}

			wsURL, err := subscribeURL(cfg)
			if err != nil {
				return err
			}
			cfg.url = wsURL

			sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt)
			defer stop()
			runCtx := sigCtx
			var cancel context.CancelFunc
			if cfg.duration > 0 {
				runCtx, cancel = context.WithTimeout(sigCtx, cfg.duration)
				defer cancel()
			}

			return run(runCtx, cfg)
		},
	}
}

type config struct {
	rawURL            string
	url               string
	concurrency       int
	reportInterval    time.Duration
	rampDuration      time.Duration
	duration          time.Duration
	dialTimeout       time.Duration
	reconnectDelay    time.Duration
	compression       bool
	cursor            string
	wantedCollections []string
	wantedDIDs        []string
	maxMessageSize    int
	requireHello      bool
	readLimit         int64
	out               io.Writer

	// dial establishes a websocket connection. It defaults to websocket.Dial
	// in run when nil; tests inject a stub to exercise dial error handling
	// without a live server.
	dial dialFunc
}

// dialFunc matches the signature of websocket.Dial so the dialer can be
// swapped out in tests.
type dialFunc func(context.Context, string, *websocket.DialOptions) (*websocket.Conn, *http.Response, error)

func (c config) validate() error {
	if c.concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if c.reportInterval <= 0 {
		return fmt.Errorf("report-interval must be > 0")
	}
	if c.rampDuration < 0 {
		return fmt.Errorf("ramp-duration must be >= 0")
	}
	if c.duration < 0 {
		return fmt.Errorf("duration must be >= 0")
	}
	if c.dialTimeout <= 0 {
		return fmt.Errorf("dial-timeout must be > 0")
	}
	if c.reconnectDelay < 0 {
		return fmt.Errorf("reconnect-delay must be >= 0")
	}
	if c.maxMessageSize < 0 {
		return fmt.Errorf("max-message-size must be >= 0")
	}
	if c.readLimit <= 0 {
		return fmt.Errorf("read-limit must be > 0")
	}
	return nil
}

func subscribeURL(c config) (string, error) {
	raw := strings.TrimSpace(c.rawURL)
	if raw == "" {
		return "", fmt.Errorf("url is required")
	}
	if !strings.Contains(raw, "://") {
		raw = "ws://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported url scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("url host is required")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/subscribe"
	}

	q := u.Query()
	if c.cursor != "" {
		q.Set("cursor", c.cursor)
	}
	for _, v := range c.wantedCollections {
		if v != "" {
			q.Add("wantedCollections", v)
		}
	}
	for _, v := range c.wantedDIDs {
		if v != "" {
			q.Add("wantedDids", v)
		}
	}
	if c.maxMessageSize > 0 {
		q.Set("maxMessageSizeBytes", strconv.Itoa(c.maxMessageSize))
	}
	if c.requireHello {
		q.Set("requireHello", "true")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

type counters struct {
	started       atomic.Int64
	connected     atomic.Int64
	events        atomic.Uint64
	bytes         atomic.Uint64
	dials         atomic.Uint64
	dialErrors    atomic.Uint64
	helloErrors   atomic.Uint64
	readErrors    atomic.Uint64
	cleanCloses   atomic.Uint64
	nonTextFrames atomic.Uint64
	reconnects    atomic.Uint64
	lastErrMu     sync.Mutex
	lastErr       string
}

func (s *counters) setLastError(format string, args ...any) {
	s.lastErrMu.Lock()
	defer s.lastErrMu.Unlock()
	s.lastErr = fmt.Sprintf(format, args...)
}

func (s *counters) lastError() string {
	s.lastErrMu.Lock()
	defer s.lastErrMu.Unlock()
	return s.lastErr
}

func run(ctx context.Context, cfg config) error {
	if cfg.dial == nil {
		cfg.dial = websocket.Dial
	}
	stats := &counters{}
	start := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, _ = fmt.Fprintf(cfg.out, "target=%d url=%s report_interval=%s ramp_duration=%s compression=%t\n",
		cfg.concurrency, cfg.url, cfg.reportInterval, cfg.rampDuration, cfg.compression)

	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		report(runCtx, cfg, stats, start)
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	launchDelay := time.Duration(0)
	if cfg.concurrency > 1 && cfg.rampDuration > 0 {
		launchDelay = cfg.rampDuration / time.Duration(cfg.concurrency-1)
	}

	for i := 0; i < cfg.concurrency; i++ {
		select {
		case <-runCtx.Done():
			goto wait
		default:
		}
		stats.started.Add(1)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := consume(runCtx, cfg, stats, id); err != nil {
				select {
				case errCh <- err:
					cancel()
				default:
				}
			}
		}(i)
		if launchDelay > 0 && i < cfg.concurrency-1 {
			select {
			case <-runCtx.Done():
				goto wait
			case <-time.After(launchDelay):
			}
		}
	}

wait:
	<-runCtx.Done()
	wg.Wait()
	<-reportDone
	printReport(cfg.out, "final", time.Since(start), cfg.concurrency, stats, reportSnapshot{}, time.Since(start))
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func consume(ctx context.Context, cfg config, stats *counters, id int) error {
	firstAttempt := true
	for {
		if err := ctx.Err(); err != nil {
			return nil //nolint:nilerr // context cancellation is a clean shutdown, not an error
		}
		if !firstAttempt {
			if !sleepReconnect(ctx, cfg.reconnectDelay, id) {
				return nil
			}
			stats.reconnects.Add(1)
		}
		firstAttempt = false

		dialCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout)
		conn, resp, err := cfg.dial(dialCtx, cfg.url, dialOptions(cfg))
		cancel()
		closeResponse(resp)
		if err != nil {
			if ctx.Err() != nil {
				return nil //nolint:nilerr // dial failed because the run context was cancelled; clean shutdown
			}
			stats.dialErrors.Add(1)
			if resp != nil {
				stats.setLastError("dial: http %d: %v", resp.StatusCode, err)
				return fmt.Errorf("subscriber %d dial: http %d: %w", id, resp.StatusCode, err)
			} else {
				stats.setLastError("dial: %v", err)
				return fmt.Errorf("subscriber %d dial: %w", id, err)
			}
		}

		stats.dials.Add(1)
		stats.connected.Add(1)
		conn.SetReadLimit(cfg.readLimit)

		if cfg.requireHello {
			if err := sendHello(ctx, conn, cfg); err != nil {
				stats.helloErrors.Add(1)
				stats.setLastError("hello: %v", err)
				stats.connected.Add(-1)
				_ = conn.Close(websocket.StatusNormalClosure, "hello failed")
				return fmt.Errorf("subscriber %d hello: %w", id, err)
			}
		}

		readLoop(ctx, conn, stats)
		stats.connected.Add(-1)
		_ = conn.CloseNow()
	}
}

func dialOptions(cfg config) *websocket.DialOptions {
	opts := &websocket.DialOptions{}
	if cfg.compression {
		opts.CompressionMode = websocket.CompressionContextTakeover
	}
	return opts
}

func sendHello(ctx context.Context, conn *websocket.Conn, cfg config) error {
	payload := optionsUpdatePayload{
		WantedCollections:   cfg.wantedCollections,
		WantedDIDs:          cfg.wantedDIDs,
		MaxMessageSizeBytes: cfg.maxMessageSize,
	}
	body, err := json.Marshal(subscriberMessage{
		Type:    "options_update",
		Payload: payload,
	})
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, body)
}

type subscriberMessage struct {
	Type    string               `json:"type"`
	Payload optionsUpdatePayload `json:"payload"`
}

type optionsUpdatePayload struct {
	WantedCollections   []string `json:"wantedCollections"`
	WantedDIDs          []string `json:"wantedDids"`
	MaxMessageSizeBytes int      `json:"maxMessageSizeBytes"`
}

func readLoop(ctx context.Context, conn *websocket.Conn, stats *counters) {
	for {
		msgType, payload, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
				stats.cleanCloses.Add(1)
				return
			}
			stats.readErrors.Add(1)
			stats.setLastError("read: %v", err)
			return
		}
		stats.bytes.Add(uint64(len(payload)))
		if msgType == websocket.MessageText {
			stats.events.Add(1)
		} else {
			stats.nonTextFrames.Add(1)
		}
	}
}

func closeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func sleepReconnect(ctx context.Context, base time.Duration, id int) bool {
	if base <= 0 {
		return true
	}
	delay := base + time.Duration(id%100)*10*time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type reportSnapshot struct {
	events uint64
	bytes  uint64
}

func report(ctx context.Context, cfg config, stats *counters, start time.Time) {
	ticker := time.NewTicker(cfg.reportInterval)
	defer ticker.Stop()

	lastAt := start
	last := reportSnapshot{}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			interval := now.Sub(lastAt)
			last = printReport(cfg.out, "stats", now.Sub(start), cfg.concurrency, stats, last, interval)
			lastAt = now
		}
	}
}

func printReport(
	w io.Writer,
	label string,
	elapsed time.Duration,
	target int,
	stats *counters,
	last reportSnapshot,
	interval time.Duration,
) reportSnapshot {
	events := stats.events.Load()
	bytes := stats.bytes.Load()
	deltaEvents := events - last.events
	deltaBytes := bytes - last.bytes

	seconds := interval.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	eps := float64(deltaEvents) / seconds
	bps := float64(deltaBytes) / seconds

	avgBytes := uint64(0)
	if events > 0 {
		avgBytes = bytes / events
	}

	lastErr := stats.lastError()
	if lastErr == "" {
		lastErr = "none"
	}

	_, _ = fmt.Fprintf(w,
		"%s elapsed=%s conns=%d/%d started=%d events=%s eps=%.0f bytes=%s throughput=%s/s avg_event=%s dials=%s dial_err=%s reconnects=%s read_err=%s hello_err=%s clean_close=%s non_text=%s last_err=%s\n",
		label,
		roundDuration(elapsed),
		stats.connected.Load(),
		target,
		stats.started.Load(),
		formatCount(events),
		eps,
		format.Bytes(int64(bytes)),
		format.Bytes(int64(bps)),
		format.Bytes(int64(avgBytes)),
		formatCount(stats.dials.Load()),
		formatCount(stats.dialErrors.Load()),
		formatCount(stats.reconnects.Load()),
		formatCount(stats.readErrors.Load()),
		formatCount(stats.helloErrors.Load()),
		formatCount(stats.cleanCloses.Load()),
		formatCount(stats.nonTextFrames.Load()),
		lastErr,
	)

	return reportSnapshot{events: events, bytes: bytes}
}

func roundDuration(d time.Duration) time.Duration {
	if d < time.Second {
		return d.Round(time.Millisecond)
	}
	return d.Round(time.Second)
}

func formatCount[T ~int64 | ~uint64](n T) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre == 0 {
		pre = 3
	}
	b.WriteString(s[:pre])
	for i := pre; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jcalabro/atmos/cbor"
)

// syncBuffer is a concurrency-safe bytes.Buffer for capturing CLI output while
// a goroutine polls it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// liveServer serves a fixed set of extended /subscribe-v2 commit frames.
func liveServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subscribe-v2" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }()
		for _, f := range frames {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(f)); err != nil {
				return
			}
		}
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func commitFrame(t *testing.T, seq uint64, did, coll, rkey string) string {
	t.Helper()
	rec, err := cbor.Marshal(map[string]any{"$type": coll, "text": "hi " + rkey})
	if err != nil {
		t.Fatal(err)
	}
	s := strconv.FormatUint(seq, 10)
	return `{"did":"` + did + `","time_us":1,"cursor":` + s + `,"seq":` + s +
		`,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"` + coll +
		`","rkey":"` + rkey + `","cid":"bafytest","record_cbor":"` + base64.StdEncoding.EncodeToString(rec) + `"}}`
}

// TestSubscribeFatalBackfillReturnsError is the E1/E2 regression guard: a
// doomed backfill (here, the server rejects planBackfill) must make the CLI
// return a non-zero error in BOTH --print and throughput modes, rather than log
// (or silently drop) the error and exit 0 — which would mask a failed backfill
// from any orchestrator checking the exit status.
func TestSubscribeFatalBackfillReturnsError(t *testing.T) {
	t.Parallel()

	// Server: planBackfill fails, so the engine aborts the backfill fatally
	// before any event is delivered.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/xrpc/network.bsky.jetstream.planBackfill":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":"InternalError","message":"boom"}`)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	for _, mode := range []string{"print", "throughput"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			app := newApp()
			app.Writer = &syncBuffer{}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			args := []string{"jetstream-client", "--host", srv.URL, "--after-seq", "0"}
			if mode == "print" {
				args = append(args, "--print")
			} else {
				// throughput mode is the default (no --print).
				args = append(args, "--report-interval", "100ms")
			}
			err := app.Run(ctx, args)
			if err == nil {
				t.Fatalf("%s mode: a fatal backfill failure must return a non-zero error, got nil", mode)
			}
			if !strings.Contains(err.Error(), "aborted") {
				t.Fatalf("%s mode: expected an 'aborted' stream error, got: %v", mode, err)
			}
		})
	}
}

// TestSubscribeRejectsNegativeCursors guards C2: a negative seq cursor must be
// rejected with an error, not silently dropped (which would turn a requested
// bounded backfill into an unbounded one with no signal).
func TestSubscribeRejectsNegativeCursors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		flag string
		val  string
	}{
		{name: "before-seq negative", flag: "--before-seq", val: "-5"},
		{name: "live-cursor negative", flag: "--live-cursor", val: "-1"},
		{name: "after-seq below sentinel", flag: "--after-seq", val: "-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			app := newApp()
			app.Writer = &syncBuffer{}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			// localhost:1 never connects; the validation must trip before any I/O.
			err := app.Run(ctx, []string{"jetstream-client", "--host", "localhost:1", tc.flag, tc.val})
			if err == nil {
				t.Fatalf("%s %s: expected a validation error, got nil", tc.flag, tc.val)
			}
			if !strings.Contains(err.Error(), tc.flag) {
				t.Fatalf("%s %s: error should name the flag, got: %v", tc.flag, tc.val, err)
			}
		})
	}
}

// TestSubscribePrintsEvents runs the real subscribe command (live-only) against
// a fake server and asserts decoded events are printed as JSON.
func TestSubscribePrintsEvents(t *testing.T) {
	t.Parallel()
	srv := liveServer(t, []string{
		commitFrame(t, 1, "did:plc:a", "app.bsky.feed.post", "r1"),
		commitFrame(t, 2, "did:plc:a", "app.bsky.feed.post", "r2"),
	})

	out := &syncBuffer{}
	app := newApp()
	app.Writer = out

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Stop shortly after both events should have arrived.
	go func() {
		// Poll the output until two JSON lines appear, then cancel.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if strings.Count(out.String(), "\n") >= 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	err := app.Run(ctx, []string{"jetstream-client", "--host", srv.URL, "--print"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected >=2 event lines, got %d:\n%s", len(lines), out.String())
	}
	var ev struct {
		Cursor uint64 `json:"cursor"`
		Kind   string `json:"kind"`
		Commit struct {
			Collection string `json:"collection"`
			Rkey       string `json:"rkey"`
		} `json:"commit"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &ev); err != nil {
		t.Fatalf("decode printed event: %v\nline: %s", err, lines[0])
	}
	if ev.Cursor != 1 || ev.Kind != "commit" || ev.Commit.Rkey != "r1" {
		t.Fatalf("unexpected first event: %+v", ev)
	}
}

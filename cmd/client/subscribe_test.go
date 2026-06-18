package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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

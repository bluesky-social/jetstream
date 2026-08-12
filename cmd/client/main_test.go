package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSubscribeURLDefaults(t *testing.T) {
	t.Parallel()

	got, err := subscribeURL(config{rawURL: "localhost:8080"})
	if err != nil {
		t.Fatal(err)
	}
	if want := "ws://localhost:8080/subscribe"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSubscribeURLPreservesPathAndExistingQuery(t *testing.T) {
	t.Parallel()

	got, err := subscribeURL(config{
		rawURL:            "ws://example.com/custom?wantedCollections=existing",
		wantedCollections: []string{"app.bsky.feed.post"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "ws://example.com/custom?wantedCollections=existing&wantedCollections=app.bsky.feed.post"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestSubscribeURLRejectsUnsupportedScheme(t *testing.T) {
	t.Parallel()

	if _, err := subscribeURL(config{rawURL: "ftp://example.com"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestSubscribeURLKindsAreV2Only(t *testing.T) {
	t.Parallel()

	_, err := subscribeURL(config{rawURL: "ws://example.com/subscribe", kinds: []string{"commit"}})
	if err == nil || !strings.Contains(err.Error(), "v1 /subscribe") {
		t.Fatalf("expected explicit v1 kind-filter rejection, got %v", err)
	}

	got, err := subscribeURL(config{
		rawURL: "ws://example.com/xrpc/network.bsky.jetstream.subscribeEvents",
		kinds:  []string{"commit", "account"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "kinds=commit") || !strings.Contains(got, "kinds=account") {
		t.Fatalf("v2 URL missing repeated kind filters: %s", got)
	}
}

func TestRunExitsWhenDialFails(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var out bytes.Buffer
	start := time.Now()
	err := run(ctx, config{
		url:            "ws://example.test/subscribe",
		concurrency:    1,
		reportInterval: time.Hour,
		dialTimeout:    100 * time.Millisecond,
		reconnectDelay: time.Hour,
		readLimit:      10_000_000,
		out:            &out,
		dial: func(context.Context, string, *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
			return nil, &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("not ready")),
			}, errors.New("rejected")
		},
	})
	if err == nil {
		t.Fatal("expected dial failure")
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("run returned too slowly after dial failure: %s", elapsed)
	}
	if !strings.Contains(err.Error(), "http 503") {
		t.Fatalf("error %q does not include HTTP status", err)
	}
	if !strings.Contains(out.String(), "final ") {
		t.Fatalf("expected final report, got:\n%s", out.String())
	}
}

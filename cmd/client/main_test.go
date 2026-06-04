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

func TestSubscribeURLConvertsHTTPAndAddsQuery(t *testing.T) {
	t.Parallel()

	got, err := subscribeURL(config{
		rawURL:            "https://example.com",
		cursor:            "123",
		wantedCollections: []string{"app.bsky.feed.post", "app.bsky.graph.*"},
		wantedDIDs:        []string{"did:plc:abc"},
		maxMessageSize:    1024,
		requireHello:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "wss://example.com/subscribe?cursor=123&maxMessageSizeBytes=1024&requireHello=true&wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.graph.%2A&wantedDids=did%3Aplc%3Aabc"
	if got != want {
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

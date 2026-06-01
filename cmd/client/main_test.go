package main

import "testing"

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

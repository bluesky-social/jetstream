package livestream

import (
	"fmt"
	"net/url"
)

// subscribeReposPath is the relay XRPC path that delivers the firehose.
const subscribeReposPath = "/xrpc/com.atproto.sync.subscribeRepos"

// deriveSubscribeReposURL converts an HTTP relay base URL (the same
// value the operator passes via --relay-url) into the WebSocket URL
// the atmos streaming client needs.
//
// Scheme mapping: https → wss, http → ws. Any path / query the
// caller might have on the input is discarded — the firehose path
// is fixed by the protocol.
func deriveSubscribeReposURL(relayURL string) (string, error) {
	if relayURL == "" {
		return "", fmt.Errorf("livestream: relay URL is empty")
	}
	parsed, err := url.Parse(relayURL)
	if err != nil {
		return "", fmt.Errorf("livestream: parse relay URL: %w", err)
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	default:
		return "", fmt.Errorf("livestream: unsupported relay scheme %q (want http or https)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("livestream: relay URL %q is missing a host", relayURL)
	}
	parsed.Path = subscribeReposPath
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

// DeriveRelayHTTPURL strips the firehose path/query/fragment from
// a relay URL and normalizes the scheme to http(s). Used by the
// sync.Client + xrpc.Client construction in cmd/jetstream — atmos
// expects an http(s) base URL for getRepo, even though the same
// operator passes wss:// or https:// for the firehose.
//
// Mirrors atp's deriveHTTPURL. Scheme mapping:
//
//	https/http → unchanged
//	wss        → https
//	ws         → http
func DeriveRelayHTTPURL(relayURL string) (string, error) {
	if relayURL == "" {
		return "", fmt.Errorf("livestream: relay URL is empty")
	}
	parsed, err := url.Parse(relayURL)
	if err != nil {
		return "", fmt.Errorf("livestream: parse relay URL: %w", err)
	}
	switch parsed.Scheme {
	case "https", "http":
		// pass through
	case "wss":
		parsed.Scheme = "https"
	case "ws":
		parsed.Scheme = "http"
	default:
		return "", fmt.Errorf("livestream: unsupported relay scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("livestream: relay URL %q is missing a host", relayURL)
	}
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

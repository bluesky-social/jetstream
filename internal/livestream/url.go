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

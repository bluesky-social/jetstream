package http

import (
	"net/url"
	"strings"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
)

type pdsTopology struct {
	relayAuthority string
	publicScheme   string
	virtual        bool
	hosts          []string
}

func newPDSTopology(w *world.World, publicURL string) pdsTopology {
	parsed, _ := url.Parse(publicURL)
	relay := "sim.invalid"
	scheme := "http"
	if parsed != nil {
		if parsed.Host != "" {
			relay = strings.ToLower(parsed.Host)
		}
		if parsed.Scheme != "" {
			scheme = parsed.Scheme
		}
	}
	virtual := publicURL != "" && strings.EqualFold(relay, "sim.invalid")
	if publicURL == "" {
		relay = ""
	}
	hosts := make([]string, 0, w.PDSHostCount())
	if virtual {
		for i := range w.PDSHostCount() {
			hosts = append(hosts, world.VirtualPDSHostname(i))
		}
	} else {
		// Standalone localhost mode keeps one routable authority. Production
		// bootstrap has no test transport that can resolve virtual names.
		hosts = append(hosts, relay)
	}
	return pdsTopology{relayAuthority: relay, publicScheme: scheme, virtual: virtual, hosts: hosts}
}

func (t pdsTopology) pdsIndex(authority string) (int, bool) {
	authority = strings.ToLower(authority)
	if !t.virtual {
		return 0, true
	}
	for i, host := range t.hosts {
		if authority == host {
			return i, true
		}
	}
	return 0, false
}

func (t pdsTopology) endpointForAccount(w *world.World, accountIdx int) string {
	if !t.virtual {
		return t.publicScheme + "://" + t.relayAuthority
	}
	return "http://" + t.hosts[w.PDSIndexForAccount(accountIdx)]
}

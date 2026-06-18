package xrpcapi

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
)

// FuzzValidatePlanCollections feeds arbitrary collection-pattern lists to the
// wildcard parser and asserts panic-freedom plus the structural invariants that
// every accepted result must satisfy: exact entries parse as NSIDs, every stored
// prefix ends in exactly one "." and never retains "*", the prefix's head
// re-probes as a valid namespace, and the distinct-pattern count never exceeds
// the cap. The corpus is seeded with happy and adversarial inputs.
func FuzzValidatePlanCollections(f *testing.F) {
	seeds := []string{
		"app.bsky.feed.post",
		"app.bsky.feed.*",
		"app.bsky.*",
		"app.*",
		".*",
		"*",
		"app.bsky..*",
		"app.bsky.feed.*.*",
		"app.bsky.fo*",
		"not a collection",
		"",
	}
	for _, s := range seeds {
		f.Add(s, s, 10)
	}

	f.Fuzz(func(t *testing.T, a, b string, maxRaw int) {
		// Bound the cap to a sane window; the function treats 0 as "disabled".
		maxCollections := maxRaw % 8
		if maxCollections < 0 {
			maxCollections = -maxCollections
		}

		raw := []string{a, b}
		exact, prefixes, err := validatePlanCollections(raw, maxCollections)
		if err != nil {
			// Rejection is always acceptable; nothing further to check.
			return
		}

		// Distinct-pattern cap must hold on success.
		if len(exact)+len(prefixes) > maxCollections {
			t.Fatalf("accepted %d patterns over cap %d (exact=%v prefixes=%v)",
				len(exact)+len(prefixes), maxCollections, exact, prefixes)
		}

		for _, ex := range exact {
			if _, perr := atmos.ParseNSID(ex); perr != nil {
				t.Fatalf("accepted exact %q is not a valid NSID: %v", ex, perr)
			}
		}
		for _, pre := range prefixes {
			if !strings.HasSuffix(pre, ".") {
				t.Fatalf("stored prefix %q does not end in '.'", pre)
			}
			if strings.Contains(pre, "*") {
				t.Fatalf("stored prefix %q retains '*'", pre)
			}
			// head = prefix without the trailing "." must re-probe as a namespace.
			head := strings.TrimSuffix(pre, ".")
			if _, perr := atmos.ParseNSID(head + ".wildcard"); perr != nil {
				t.Fatalf("stored prefix head %q does not re-probe as a namespace: %v", head, perr)
			}
		}
	})
}

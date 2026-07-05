package http

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/jcalabro/atmos"
)

// didDoc is a minimal DID document shape compatible with what
// atmos.identity.DefaultResolver expects. We don't use the atmos
// identity types directly because they have private fields that
// don't round-trip through encoding/json reliably.
type didDoc struct {
	ID                 string               `json:"id"`
	AlsoKnownAs        []string             `json:"alsoKnownAs"`
	VerificationMethod []verificationMethod `json:"verificationMethod"`
	Service            []service            `json:"service"`
}

type verificationMethod struct {
	ID                 string `json:"id"`
	Type               string `json:"type"`
	Controller         string `json:"controller"`
	PublicKeyMultibase string `json:"publicKeyMultibase"`
}

type service struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

// newPLCHandler returns a handler matching atmos's PLC resolution
// pattern: GET <plcURL>/<did> → JSON DID document.
func newPLCHandler(w *world.World, pdsEndpoint string, faults *FaultPlan) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		didStr := strings.TrimPrefix(r.URL.Path, "/")
		did, err := atmos.ParseDID(didStr)
		if err != nil {
			http.Error(rw, "bad did", http.StatusBadRequest)
			return
		}
		acct, ok, err := w.FindAccountByDID(did)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.NotFound(rw, r)
			return
		}
		mode, faulted := faults.maybePLCFault(string(did))
		if faulted && mode == PLCFaultResolutionFailure {
			http.Error(rw, "simulated PLC resolution failure", http.StatusServiceUnavailable)
			return
		}
		handle := "at://user-" + acct.HandleSuffix() + ".test"
		serviceEndpoint := pdsEndpoint
		if faulted {
			switch mode {
			case PLCFaultMalformedHandle:
				handle = "not an at-uri"
			case PLCFaultMalformedPDSEndpoint:
				serviceEndpoint = "://not-a-url"
			}
		}
		doc := didDoc{
			ID:          string(acct.DID),
			AlsoKnownAs: []string{handle},
			VerificationMethod: []verificationMethod{{
				ID:                 string(acct.DID) + "#atproto",
				Type:               "Multikey",
				Controller:         string(acct.DID),
				PublicKeyMultibase: acct.PubkeyMultibase(),
			}},
		}
		if !faulted || mode != PLCFaultMissingPDSEndpoint {
			doc.Service = []service{{
				ID:              "#atproto_pds",
				Type:            "AtprotoPersonalDataServer",
				ServiceEndpoint: serviceEndpoint,
			}}
		}
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(doc)
	})
}

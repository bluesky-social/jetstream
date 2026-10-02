// Package datamodel checks upstream record bytes against the atproto data
// model at the ingest gate (docs/README.md §4.4).
//
// Every read path renders a record as atproto JSON: the v1 and v2 websocket
// encoders through cbor.ToJSON, and the Go client through its own decoder,
// which is the strictest of the three. A record that passes CheckRecord is
// accepted by all of them. A record that fails would be archived but either
// skipped by the websocket encoders or rejected by the client, so the two
// paths would disagree about which events exist.
package datamodel

import (
	"errors"
	"fmt"

	"github.com/jcalabro/atmos/cbor"
)

// ErrInvalid is wrapped by every CheckRecord error.
var ErrInvalid = errors.New("record is outside the atproto data model")

// CheckRecord reports whether payload is one DAG-CBOR value, with no trailing
// bytes, that is a map holding only data-model values. The CBOR decoder
// already rejects CIDs other than CIDv1 dag-cbor/raw SHA-256, NaN and
// infinities; floats are rejected here because atproto has integers only.
func CheckRecord(payload []byte) error {
	val, err := cbor.UnmarshalNoCopy(payload)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	m, ok := val.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: record is a %T, not a map", ErrInvalid, val)
	}
	if err := checkMap(m); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

func checkMap(m map[string]any) error {
	for k, v := range m {
		if err := checkValue(v); err != nil {
			return fmt.Errorf("field %q: %w", k, err)
		}
	}
	return nil
}

func checkValue(v any) error {
	switch val := v.(type) {
	case nil, bool, int64, string, []byte, cbor.CID:
		return nil
	case []any:
		for i, e := range val {
			if err := checkValue(e); err != nil {
				return fmt.Errorf("item %d: %w", i, err)
			}
		}
		return nil
	case map[string]any:
		return checkMap(val)
	default:
		return fmt.Errorf("unsupported value %T", v)
	}
}

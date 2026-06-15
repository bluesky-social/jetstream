package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/bluesky-social/jetstream-v2/segment"
)

type EventLogRow struct {
	Seq              uint64 `json:"seq"`
	Kind             string `json:"kind"`
	DID              string `json:"did,omitempty"`
	Collection       string `json:"collection,omitempty"`
	Rkey             string `json:"rkey,omitempty"`
	Rev              string `json:"rev,omitempty"`
	PayloadLen       int    `json:"payload_len,omitempty"`
	PayloadSHA256_64 string `json:"payload_sha256_64,omitempty"`
}

func NormalizeEventLog(events []ObservedEvent) []EventLogRow {
	out := make([]EventLogRow, 0, len(events))
	for _, ev := range events {
		row := EventLogRow{
			Seq:        ev.Seq,
			Kind:       eventLogKind(ev.Kind),
			DID:        ev.DID,
			Collection: ev.Collection,
			Rkey:       ev.Rkey,
			Rev:        ev.Rev,
		}
		if ev.Payload != nil {
			sum := sha256.Sum256(ev.Payload)
			row.PayloadLen = len(ev.Payload)
			row.PayloadSHA256_64 = hex.EncodeToString(sum[:8])
		}
		out = append(out, row)
	}
	return out
}

func CompareEventLogs(want, got []EventLogRow) error {
	for i := 0; i < len(want) && i < len(got); i++ {
		if want[i] == got[i] {
			continue
		}
		wantContainsGot := eventLogContains(want[i+1:], got[i])
		gotContainsWant := eventLogContains(got[i+1:], want[i])
		if wantContainsGot && gotContainsWant {
			return fmt.Errorf("oracle: event order mismatch at index %d: want %s got %s",
				i, want[i].describe(), got[i].describe())
		}
		if wantContainsGot {
			return fmt.Errorf("oracle: missing expected event at index %d: %s", i, want[i].describe())
		}
		if gotContainsWant {
			return fmt.Errorf("oracle: extra observed event at index %d: %s", i, got[i].describe())
		}
		return fmt.Errorf("oracle: event mismatch at index %d: want %s got %s%s",
			i, want[i].describe(), got[i].describe(), eventLogMismatchFields(want[i], got[i]))
	}
	if len(want) > len(got) {
		return fmt.Errorf("oracle: missing expected event at index %d: %s", len(got), want[len(got)].describe())
	}
	if len(got) > len(want) {
		return fmt.Errorf("oracle: extra observed event at index %d: %s", len(want), got[len(want)].describe())
	}
	return nil
}

func eventLogContains(rows []EventLogRow, target EventLogRow) bool {
	for _, row := range rows {
		if row == target {
			return true
		}
	}
	return false
}

func eventLogMismatchFields(want, got EventLogRow) string {
	var out string
	if want.Seq != got.Seq {
		out += " seq"
	}
	if want.Kind != got.Kind {
		out += " kind"
	}
	if want.DID != got.DID {
		out += " did"
	}
	if want.Collection != got.Collection || want.Rkey != got.Rkey {
		out += " key"
	}
	if want.Rev != got.Rev {
		out += " rev"
	}
	if want.PayloadLen != got.PayloadLen || want.PayloadSHA256_64 != got.PayloadSHA256_64 {
		out += " payload"
	}
	if out == "" {
		return ""
	}
	return " fields:" + out
}

func (r EventLogRow) describe() string {
	return fmt.Sprintf("seq=%d kind=%s did=%s key=%s/%s rev=%s payload_len=%d payload_sha256_64=%s",
		r.Seq, r.Kind, r.DID, r.Collection, r.Rkey, r.Rev, r.PayloadLen, r.PayloadSHA256_64)
}

func eventLogKind(kind segment.Kind) string {
	switch kind {
	case segment.KindCreate:
		return "create"
	case segment.KindUpdate:
		return "update"
	case segment.KindDelete:
		return "delete"
	case segment.KindIdentity:
		return "identity"
	case segment.KindAccount:
		return "account"
	case segment.KindSync:
		return "sync"
	default:
		return fmt.Sprintf("unknown-%d", kind)
	}
}

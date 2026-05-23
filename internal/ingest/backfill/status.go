package backfill

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jcalabro/atmos"
)

// repoKeyPrefix is the pebble key prefix for per-DID rows. DESIGN.md
// §3.5 pins this layout so the on-disk format is stable across
// replicas.
const repoKeyPrefix = "repo/"

// repoKey returns the pebble key for a DID's RepoStatus row.
func repoKey(did atmos.DID) []byte {
	return []byte(repoKeyPrefix + string(did))
}

// Status is the lifecycle state of a single DID's initial backfill.
// Values match DESIGN.md §3.5; the StatusNotStarted value is what
// OnDiscover writes — a row's mere existence at not_started indicates
// the engine has seen it but not yet downloaded it.
type Status string

const (
	StatusNotStarted Status = "not_started"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
)

// RepoBackfillStatus tracks initial-backfill state per DESIGN.md §3.5.
type RepoBackfillStatus struct {
	Status      Status    `json:"status"`
	Rev         string    `json:"rev,omitempty"`
	Attempts    int       `json:"attempts,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	StartedAt   time.Time `json:"started_at,omitzero"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
}

// RepoStatus is the JSON value stored at repo/<did>. The shape matches
// DESIGN.md §3.5; this PR only populates Backfill and Active. The
// other fields (PDS, Rev, UpdatedAt, RecordCount, TotalBytes) are
// reserved for steady-state ingest and remain zero here so we don't
// force a future schema migration.
//
// Active records the last-observed listRepos.Active value. atmos
// requires it on every row to detect liveness flips without an extra
// round-trip; DESIGN.md §3.5 doesn't pin a JSON tag for it (the
// original draft predated atmos's active-flip callback) so we add one
// here.
type RepoStatus struct {
	Backfill    RepoBackfillStatus `json:"backfill"`
	PDS         string             `json:"pds,omitempty"`
	Rev         string             `json:"rev,omitempty"`
	UpdatedAt   time.Time          `json:"updated_at,omitzero"`
	RecordCount int64              `json:"record_count,omitempty"`
	TotalBytes  int64              `json:"total_bytes,omitempty"`
	Active      bool               `json:"active"`
}

// encodeRepoStatus marshals a RepoStatus for persistence. Errors here
// are programming bugs (the type is a fixed shape we control), but we
// surface them rather than panicking so the engine can record a Run
// failure and exit cleanly.
func encodeRepoStatus(s *RepoStatus) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("backfill: encode RepoStatus: %w", err)
	}
	return b, nil
}

// decodeRepoStatus unmarshals a previously-stored RepoStatus.
func decodeRepoStatus(b []byte) (*RepoStatus, error) {
	var s RepoStatus
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("backfill: decode RepoStatus: %w", err)
	}
	return &s, nil
}

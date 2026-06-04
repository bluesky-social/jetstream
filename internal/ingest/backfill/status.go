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
// other fields (PDS, Host, Handle, Rev, UpdatedAt, LastAttemptedAt,
// RecordCount, TotalBytes) are reserved for steady-state ingest and
// diagnostics and remain zero here so we don't force a future schema
// migration.
//
// Active records the last-observed listRepos.Active value. atmos
// requires it on every row to detect liveness flips without an extra
// round-trip; DESIGN.md §3.5 doesn't pin a JSON tag for it (the
// original draft predated atmos's active-flip callback) so we add one
// here.
type RepoStatus struct {
	Backfill        RepoBackfillStatus `json:"backfill"`
	PDS             string             `json:"pds,omitempty"`
	Host            string             `json:"host,omitempty"`
	Handle          string             `json:"handle,omitempty"`
	Rev             string             `json:"rev,omitempty"`
	UpdatedAt       time.Time          `json:"updated_at,omitzero"`
	LastAttemptedAt time.Time          `json:"last_attempted_at,omitzero"`
	RecordCount     int64              `json:"record_count,omitempty"`
	TotalBytes      int64              `json:"total_bytes,omitempty"`
	Active          bool               `json:"active"`
}

// ErrorClass is a coarse backfill failure bucket for host/account
// diagnostics. It is intentionally lower-cardinality than raw errors
// so dashboards can group failures without parsing arbitrary strings.
type ErrorClass string

const (
	ErrorClassUnknown       ErrorClass = "unknown"
	ErrorClassDIDResolution ErrorClass = "did_resolution"
	ErrorClassInvalidPDS    ErrorClass = "invalid_pds"
	ErrorClassHTTP429       ErrorClass = "http_429"
	ErrorClassHTTP5xx       ErrorClass = "http_5xx"
	ErrorClassTimeout       ErrorClass = "timeout"
	ErrorClassCAR           ErrorClass = "car"
	ErrorClassVerification  ErrorClass = "verification"
	ErrorClassLocalWrite    ErrorClass = "local_write"
)

// HostErrorSample stores one recent account-level error for a host
// bucket. Error is bounded before persistence to protect the metadata
// store from high-cardinality or adversarially large error strings.
type HostErrorSample struct {
	DID         atmos.DID  `json:"did"`
	AttemptedAt time.Time  `json:"attempted_at,omitzero"`
	Class       ErrorClass `json:"class"`
	Error       string     `json:"error,omitempty"`
}

// HostStatus is the JSON value stored at host/<bucket> for diagnostic
// dashboards. It is derived metadata; repo/<did> remains the source of
// truth for individual account lifecycle state.
type HostStatus struct {
	Host             string                `json:"host"`
	Total            uint64                `json:"total"`
	Active           uint64                `json:"active"`
	NotStarted       uint64                `json:"not_started"`
	Complete         uint64                `json:"complete"`
	Failed           uint64                `json:"failed"`
	LastAttemptedAt  time.Time             `json:"last_attempted_at,omitzero"`
	LatestError      string                `json:"latest_error,omitempty"`
	LatestErrorClass ErrorClass            `json:"latest_error_class,omitempty"`
	ErrorClassCounts map[ErrorClass]uint64 `json:"error_class_counts,omitempty"`
	RecentErrors     []HostErrorSample     `json:"recent_errors,omitempty"`
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

// DecodeRepoStatus is the exported decoder used by cross-package
// readers (the orchestrator's merge phase) that need to read the
// JSON shape stored at repo/<did>. Internal callers continue to
// use decodeRepoStatus directly.
func DecodeRepoStatus(b []byte) (*RepoStatus, error) {
	return decodeRepoStatus(b)
}

// EncodeRepoStatus is the exported encoder used by cross-package
// writers (the orchestrator's merge phase committing per-DID Rev
// updates) that need to produce the JSON shape stored at
// repo/<did>. Internal callers continue to use encodeRepoStatus
// directly.
func EncodeRepoStatus(s *RepoStatus) ([]byte, error) {
	return encodeRepoStatus(s)
}

// RepoKey returns the pebble key for a DID's RepoStatus row. Mirror
// of the unexported repoKey; exported for cross-package writers.
func RepoKey(did string) []byte {
	return []byte(repoKeyPrefix + did)
}

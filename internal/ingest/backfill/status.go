package backfill

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

// repoKeyPrefix is the pebble key prefix for per-DID rows. docs/README.md
// §3.5 pins this layout so the on-disk format is stable across
// replicas.
const repoKeyPrefix = "repo/"
const pdsHostKeyPrefix = "pdshost/"

// repoKey returns the pebble key for a DID's RepoStatus row.
func repoKey(did atmos.DID) []byte {
	return []byte(repoKeyPrefix + string(did))
}

func pdsHostKey(hostname string) []byte { return []byte(pdsHostKeyPrefix + hostname) }

// PDSHost is the durable control-plane roster row for one listHosts entry.
// RelayAccounts is only a relay-observed floor. ActualAccounts counts DIDs
// first discovered on this host during a crawl; migration-window duplicates
// already attributed to another host are deliberately not double-counted.
type PDSHost struct {
	Hostname           string    `json:"hostname"`
	RelayStatus        string    `json:"relay_status,omitempty"`
	RelayAccounts      int64     `json:"relay_accounts,omitempty"`
	Seq                int64     `json:"seq,omitempty"`
	ListReposCursor    string    `json:"list_repos_cursor,omitempty"`
	LastNonEmptyCursor string    `json:"last_non_empty_cursor,omitempty"`
	Enumerated         bool      `json:"enumerated"`
	ActualAccounts     uint64    `json:"actual_accounts,omitempty"`
	Attempts           int       `json:"attempts,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	NextAttemptAt      time.Time `json:"next_attempt_at,omitzero"`
	State              string    `json:"state,omitempty"`
	FirstSeenAt        time.Time `json:"first_seen_at,omitzero"`
	UpdatedAt          time.Time `json:"updated_at,omitzero"`
}

func encodePDSHost(host *PDSHost) ([]byte, error) {
	b, err := json.Marshal(host)
	if err != nil {
		return nil, fmt.Errorf("backfill: encode PDSHost: %w", err)
	}
	return b, nil
}

func decodePDSHost(b []byte) (*PDSHost, error) {
	var host PDSHost
	if err := json.Unmarshal(b, &host); err != nil {
		return nil, fmt.Errorf("backfill: decode PDSHost: %w", err)
	}
	return &host, nil
}

// ListPDSHosts returns the durable control-plane roster ordered by hostname.
// A malformed row is Jetstream-owned metadata corruption and aborts the scan.
func ListPDSHosts(db *store.Store) ([]PDSHost, error) {
	prefix := []byte(pdsHostKeyPrefix)
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: store.PrefixUpperBound(prefix)})
	if err != nil {
		return nil, fmt.Errorf("backfill: open PDS roster: %w", err)
	}
	defer func() { _ = it.Close() }()
	var hosts []PDSHost
	for it.First(); it.Valid(); it.Next() {
		value, err := it.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("backfill: read PDS roster: %w", err)
		}
		host, err := decodePDSHost(value)
		if err != nil {
			return nil, err
		}
		if host.Hostname == "" {
			host.Hostname = string(it.Key()[len(prefix):])
		}
		hosts = append(hosts, *host)
	}
	if err := it.Error(); err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("backfill: iterate PDS roster: %w", err)
	}
	return hosts, nil
}

// Status is the lifecycle state of a single DID's initial backfill.
// Values match docs/README.md §3.5; the StatusNotStarted value is what
// OnDiscover writes — a row's mere existence at not_started indicates
// the engine has seen it but not yet downloaded it.
type Status string

const (
	StatusNotStarted Status = "not_started"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
	// StatusPending is a DID awaiting a whole-repo replacement through the
	// explicit pending retry pass. Bootstrap crash recovery promotes a
	// pre-existing StatusNotStarted row to pending instead of re-downloading
	// it at low seqs (#262); merge then retries it after the captured live tail
	// lands. Older stores may also contain pending rows from the removed
	// live-first-sighting enqueue path, but new live first sightings must not
	// create pending rows.
	StatusPending Status = "pending"
	// StatusUnavailable is a terminal, non-failure state: the account
	// exists but its repo cannot be fetched (deactivated, suspended, or
	// taken down per com.atproto.sync.getRepo). Unlike StatusFailed it
	// is never retried — Lookup projects it to atmos StateComplete so
	// the engine skips re-dispatch — and it is tracked separately from
	// StatusComplete so dashboards don't conflate "downloaded" with
	// "nothing to download".
	StatusUnavailable Status = "unavailable"
)

// RepoBackfillStatus tracks initial-backfill state per docs/README.md §3.5.
type RepoBackfillStatus struct {
	Status        Status    `json:"status"`
	Rev           string    `json:"rev,omitempty"`
	Attempts      int       `json:"attempts,omitempty"`
	RetryCount    int       `json:"retry_count,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitzero"`
	StartedAt     time.Time `json:"started_at,omitzero"`
	CompletedAt   time.Time `json:"completed_at,omitzero"`
}

// RepoStatus is the JSON value stored at repo/<did>. The shape matches
// docs/README.md §3.5; this PR only populates Backfill and Active. The
// other fields (PDS, Host, Handle, Rev, UpdatedAt, LastAttemptedAt,
// RecordCount, TotalBytes) are reserved for steady-state ingest and
// diagnostics and remain zero here so we don't force a future schema
// migration.
//
// Active records the last-observed listRepos.Active value. atmos
// requires it on every row to detect liveness flips without an extra
// round-trip; docs/README.md §3.5 doesn't pin a JSON tag for it (the
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
	Pending          uint64                `json:"pending"`
	Complete         uint64                `json:"complete"`
	Failed           uint64                `json:"failed"`
	Unavailable      uint64                `json:"unavailable"`
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

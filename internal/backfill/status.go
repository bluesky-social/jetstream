package backfill

import "time"

// Status is the per-DID backfill state machine value from DESIGN.md
// §3.5. Values are the on-disk strings; the order they're declared
// in is also the order they progress through.
type Status string

const (
	// StatusNotStarted is the initial state: the DID is known to the
	// relay (it appeared in com.atproto.sync.listRepos) but we have
	// not yet attempted to download its repo.
	StatusNotStarted Status = "not_started"

	// StatusComplete means the initial repo download finished
	// successfully. The corresponding RepoBackfillStatus.Rev is the
	// rev at the end of that download and is also written to
	// backfill_complete.log per DESIGN.md §3.5.
	StatusComplete Status = "complete"

	// StatusFailed means the initial repo download exhausted its
	// retry budget. The steady-state phase periodically retries
	// failed DIDs with backoff (DESIGN.md §4.3).
	StatusFailed Status = "failed"
)

// RepoStatus mirrors the JSON-encoded value at the `repo/<did>` key
// described in DESIGN.md §3.5. It carries both initial-backfill
// state (Backfill) and steady-state bookkeeping that ingest updates
// on every commit.
//
// Field tags match the spec exactly so on-disk values are
// inter-readable with future replicas / external tools.
type RepoStatus struct {
	Backfill RepoBackfillStatus `json:"backfill"`

	// PDS is the resolved personal data server URL last seen for
	// the account (e.g. "https://shimeji.us-east.host.bsky.network").
	// Empty until at least one resolution has succeeded.
	PDS string `json:"pds,omitempty"`

	// Rev tracks the latest rev observed in any commit for this DID.
	// Distinct from Backfill.Rev, which freezes at the end of the
	// initial download.
	Rev string `json:"rev,omitempty"`

	// UpdatedAt is the wall-clock time at which any field of this
	// struct was last written.
	UpdatedAt time.Time `json:"updated_at,omitzero"`

	// RecordCount and TotalBytes are running totals across the DID's
	// segment-resident events. They're maintained by the steady-state
	// ingest path; the bootstrap step in this package leaves them at
	// zero.
	RecordCount int64 `json:"record_count,omitempty"`
	TotalBytes  int64 `json:"total_bytes,omitempty"`
}

// RepoBackfillStatus is the embedded view of a DID's initial-download
// progress. See DESIGN.md §3.5.
type RepoBackfillStatus struct {
	Status Status `json:"status"`

	// Rev captures the rev at the moment the initial download
	// finished. Used during the merge phase (DESIGN.md §4.2) to
	// drop already-seen events from the live shadow shards.
	Rev string `json:"rev,omitempty"`

	Attempts    int       `json:"attempts,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	StartedAt   time.Time `json:"started_at,omitzero"`
	CompletedAt time.Time `json:"completed_at,omitzero"`
}

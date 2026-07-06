package oracle

// Harness pieces for the adversarial ingest-gate phase of the default
// lifecycle (#204). The world tells verifier-consistent lies; these
// helpers inject them deterministically (≥1 per mode, never
// seed-luck), exempt whole-event drops from the cursor accounting they
// intentionally violate, and assert the per-(source, reason)
// drop-counter floors that prove each lie actually fired.
//
// Account discipline (why the harness hands out THREE distinct
// accounts):
//
//   - acctOp carries the live per-op lies. Sharable with the honest
//     sync-divergence account: a later resync re-serves the lie
//     records and the gate re-drops them on the resync path — extra
//     coverage, and the counter floors are ≥, not ==.
//   - acctSyncLie must be QUIESCENT after its lie: the adversarial
//     #sync's silently-created record is permanently unarchivable by
//     design, and any later resync of the account would re-materialize
//     it, contradicting the ledger's ground-truth exclusion. It also
//     emits the window's LAST frame so no later honest commit on the
//     shared sync-div account can skew that account's replacement-row
//     snapshot.
//   - acctVerifier hosts the verifier-rejected commit + honest
//     follow-up OUTSIDE the event-log compare window (the follow-up is
//     silently swallowed by the verifier's chain-break resync, so its
//     seq never produces archived rows — same shape as the existing
//     async-resync divergence, which also lives outside the window).

import (
	"net/http"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/stretchr/testify/require"
)

// adversarialLiveOpLies is the live per-op lie set, covering every
// drop reason the live op gate owns. Keys must be MST-insertable and
// wire-safe (valid UTF-8): the invalid-UTF-8 class is backfill-only
// and lives in adversarialBackfillLies.
var adversarialLiveOpLies = []struct {
	name   string
	badKey string
	reason string
}{
	{"null_byte_rkey", "app.bsky.feed.post/bad\x00key", "invalid_rkey"},
	{"dotdot_rkey", "app.bsky.feed.post/..", "invalid_rkey"},
	{"rkey_over_512", "app.bsky.feed.post/" + longRunOfX(600), "invalid_rkey"},
	{"dollar_collection", "$account/3lzzzzzzzzz2a", "invalid_collection"},
	{"no_slash_path", "nosslashatall", "invalid_collection"},
	{"unicode_collection", "app.bskÿ.feed.post/3lzzzzzzzzz2a", "invalid_collection"},
	{"unrepresentable_rkey_300", "app.bsky.feed.post/" + longRunOfX(300), "field_too_long"},
}

// adversarialBackfillLies ride account MSTs into getRepo downloads.
// Injected pre-bootstrap so the ordinary backfill walk hits them; the
// account's honest records double as the survivors probe.
var adversarialBackfillLies = []struct {
	name   string
	badKey string
	reason string
}{
	{"invalid_utf8_rkey", "app.bsky.feed.post/bad\xff\xfekey", "invalid_rkey"},
	{"dotdot_rkey", "app.bsky.feed.post/..", "invalid_rkey"},
	{"no_slash_path", "nosslashbackfill", "invalid_collection"},
	{"unrepresentable_rkey_300", "app.bsky.feed.post/" + strings.Repeat("y", 300), "field_too_long"},
}

func longRunOfX(n int) string { return strings.Repeat("x", n) }

// adversarialGateReasonMatrix is the anti-vacuity contract: every
// (source, reason) pair the #204 scenario modes must light up. A zero
// floor for any of these fails the run — a mode was lost.
var adversarialGateReasonMatrix = map[string][]string{
	"live":     {"invalid_rev", "invalid_collection", "invalid_rkey", "field_too_long"},
	"backfill": {"invalid_collection", "invalid_rkey", "field_too_long"},
}

// pickAdversarialAccounts returns three distinct fetchable, active accounts
// (acctOp, acctSyncLie, acctVerifier). Fast mode keeps accounts 1-3 in that
// set after bootstrap deletes account 0; later deterministic fixtures use
// higher account indexes so they do not steal an adversarial role.
func pickAdversarialAccounts(t *testing.T, w *world.World, cfg Config) (acctOp, acctSyncLie, acctVerifier int) {
	t.Helper()
	var picked []int
	for idx := 0; idx < cfg.Accounts && len(picked) < 3; idx++ {
		if oracleAccountFetchable(t, w, idx) {
			picked = append(picked, idx)
		}
	}
	require.Lenf(t, picked, 3, "adversarial phase needs 3 fetchable active accounts, mode=%s has fewer", cfg.Mode)
	return picked[0], picked[1], picked[2]
}

func oracleAccountFetchable(t *testing.T, w *world.World, idx int) bool {
	t.Helper()
	deleted, err := w.IsAccountDeleted(idx)
	require.NoError(t, err)
	if deleted {
		return false
	}
	_, unavailable, err := w.RepoUnavailableStatus(idx)
	require.NoError(t, err)
	return !unavailable
}

// injectAdversarialBackfillLies commits the backfill lie set into
// account idx's repo. Must run before jetstream bootstraps.
func injectAdversarialBackfillLies(t *testing.T, w *world.World, idx int) {
	t.Helper()
	for _, lie := range adversarialBackfillLies {
		require.NoErrorf(t, w.InjectAdversarialRecordForBackfill(t.Context(), idx, lie.badKey, lie.reason),
			"inject backfill lie %s", lie.name)
	}
}

// injectAdversarialLiveOpLies emits one adversarial #commit per lie on
// acctOp, each carrying a benign sibling (the survivors probe the
// event-log compare asserts archived).
func injectAdversarialLiveOpLies(t *testing.T, w *world.World, cfg Config, acctOp int) {
	t.Helper()
	for _, lie := range adversarialLiveOpLies {
		_, err := w.GenerateAdversarialOpForTest(t.Context(), acctOp, lie.badKey, lie.reason)
		require.NoErrorf(t, err, "adversarial op lie %s: mode=%s seed=%d", lie.name, cfg.Mode, cfg.Seed)
	}
}

// injectVerifierRejectedCommitAndRepair emits the verifier-owned lie
// (signed-in non-TID rev) followed by the honest commit whose
// chain-break forces the verifier's async resync repair. Returns the
// (did, rev) the repair's KindSync tombstone will carry so the caller
// can await it on a syncTombstoneAck.
func injectVerifierRejectedCommitAndRepair(t *testing.T, w *world.World, cfg Config, acctVerifier int) (repairDID, repairRev string) {
	t.Helper()

	_, err := w.GenerateVerifierRejectedCommitForTest(t.Context(), acctVerifier, "not-a-tid", "non_tid_rev")
	require.NoErrorf(t, err, "verifier-rejected commit lie: mode=%s seed=%d", cfg.Mode, cfg.Seed)

	// Fixed spec-valid rkey: targeted creates fail loud on collision
	// and honest traffic only generates TID rkeys, so this cannot
	// silently mutate something else.
	_, op, err := w.GenerateRecordOpForTest(t.Context(), acctVerifier, "create", "app.bsky.feed.post", "adversarial-repair-probe")
	require.NoErrorf(t, err, "verifier-lie honest follow-up: mode=%s seed=%d", cfg.Mode, cfg.Seed)

	entry, _, err := w.ListReposPage(acctVerifier, 1)
	require.NoError(t, err)
	require.Len(t, entry, 1)
	return string(entry[0].DID), op.Rev
}

// exemptWholeEventSeqs marks every ledgered whole-event seq as
// satisfied on the ack: the consumer counts and advances past those
// events, but no archived row ever carries their cursor, so the
// gap-free wait would otherwise deadlock on an intentional drop.
func exemptWholeEventSeqs(ack *seqAck, entries []world.AdversarialEntry) {
	for _, e := range entries {
		if e.WholeEvent && e.Seq > 0 {
			ack.Exempt(e.Seq)
		}
	}
}

// assertNoPermanentCursorGapExcept is assertNoPermanentCursorGap with
// the ledger's whole-event seqs excused — their cursors are advanced
// past but never archived, by contract — plus the inverse assertion:
// an exempted seq that DID produce archived rows means the gate
// archived an event it was required to drop whole.
func assertNoPermanentCursorGapExcept(t *testing.T, observed *eventLogRecorder, after, through int64, cfg Config, phase string, filter *adversarialFilter) {
	t.Helper()

	seen := observed.ObservedUpstreamCursors(after, through)
	var missing []int64
	for c := after + 1; c <= through; c++ {
		if _, ok := seen[c]; ok {
			if filter.IsWholeEventSeq(c) {
				t.Fatalf("%s mode=%s seed=%d: whole-event seq %d was archived — the gate failed to drop an event the ledger marks as entirely invalid",
					phase, cfg.Mode, cfg.Seed, c)
			}
			continue
		}
		if filter.IsWholeEventSeq(c) {
			continue
		}
		missing = append(missing, c)
	}
	require.Emptyf(t, missing,
		"%s mode=%s seed=%d: %d upstream cursor(s) permanently lost in (%d,%d] — a graceful-cutover/backpressure regression dropped in-flight frames (first missing: %v)",
		phase, cfg.Mode, cfg.Seed, len(missing), after, through, firstN(missing, 10))
}

const oracleDebugBaseURL = "http://debug.invalid"

// assertAdversarialDropCounters proves every scheduled lie fired: for
// each ledgered gate-owned (source, reason), the absolute counter must
// be ≥ the ledger floor (the runtime registry is process-fresh, so
// absolute values ARE this run's deltas), and the full reason matrix
// must be covered. Callers must reach bubble quiescence (drain) first:
// a whole-event drop produces no ack-visible row, so without quiescence
// the scrape can race the consumer processing the lie.
func assertAdversarialDropCounters(t *testing.T, trace *Trace, w *world.World, cfg Config, client *http.Client, phase string) {
	t.Helper()

	entries := w.AdversarialLedger().Entries()
	require.NotEmptyf(t, entries, "%s: adversarial ledger is empty — the phase injected nothing (vacuous)", phase)
	floors := ExpectedDropFloors(entries)

	for source, reasons := range adversarialGateReasonMatrix {
		for _, reason := range reasons {
			require.Positivef(t, floors[source][reason],
				"%s mode=%s seed=%d: no ledgered lie for (%s,%s) — the scenario matrix lost a mode (vacuous coverage)",
				phase, cfg.Mode, cfg.Seed, source, reason)
		}
	}

	counters, err := ScrapeDropCounters(t.Context(), client, oracleDebugBaseURL)
	require.NoErrorf(t, err, "%s: scrape drop counters: mode=%s seed=%d", phase, cfg.Mode, cfg.Seed)

	observed := map[string]map[string]float64{}
	for source, reasons := range floors {
		observed[source] = map[string]float64{}
		for reason, floor := range reasons {
			got := counters[source][reason]
			observed[source][reason] = got
			require.GreaterOrEqualf(t, got, float64(floor),
				"%s mode=%s seed=%d: dropped_events_total{source=%s,reason=%s} is %v, ledger requires ≥%d — a scheduled lie was not dropped by the gate",
				phase, cfg.Mode, cfg.Seed, source, reason, got, floor)
		}
	}
	recordTraceOrError(t, trace, "adversarial_drop_counters", map[string]any{
		"phase":    phase,
		"counters": observed,
	})
}

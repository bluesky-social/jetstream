package oracle

import (
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	metastore "github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	atmosbackfill "github.com/jcalabro/atmos/backfill"
	"github.com/stretchr/testify/require"
)

const (
	oracleAccountStatusLifecycleIndex = 7
	oracleAccountStatusRkey           = "account-status-probe"
)

type oracleUnavailableRepo struct {
	accountIdx int
	status     string
}

var oracleUnavailableRepos = []oracleUnavailableRepo{
	{accountIdx: 4, status: "takendown"},
	{accountIdx: 5, status: "suspended"},
	{accountIdx: 6, status: "deactivated"},
}

var oracleNonDeletedAccountStatuses = []string{
	"takendown",
	"suspended",
	"deactivated",
	"unknown",
}

func configureOracleUnavailableRepos(t *testing.T, w *world.World, cfg Config, trace *Trace) {
	t.Helper()
	require.Greaterf(t, cfg.Accounts, oracleAccountStatusLifecycleIndex,
		"oracle account-status fixtures require at least %d accounts; mode=%s has %d",
		oracleAccountStatusLifecycleIndex+1, cfg.Mode, cfg.Accounts)

	configured := make(map[int]string, len(oracleUnavailableRepos))
	for _, tc := range oracleUnavailableRepos {
		require.NoErrorf(t, w.SetRepoUnavailableForTest(tc.accountIdx, tc.status),
			"configure unavailable repo account=%d status=%s mode=%s seed=%d",
			tc.accountIdx, tc.status, cfg.Mode, cfg.Seed)
		configured[tc.accountIdx] = tc.status
	}
	recordTraceOrError(t, trace, "repo_unavailable_config", map[string]any{
		"accounts": configured,
	})
}

func injectOracleAccountStatusLifecycle(t *testing.T, w *world.World, cfg Config) string {
	t.Helper()
	require.Truef(t, oracleAccountFetchable(t, w, oracleAccountStatusLifecycleIndex),
		"account-status lifecycle fixture account %d must stay fetchable", oracleAccountStatusLifecycleIndex)

	acct, err := w.LoadAccount(oracleAccountStatusLifecycleIndex)
	require.NoError(t, err)
	_, _, err = w.GenerateRecordOpForTest(t.Context(), oracleAccountStatusLifecycleIndex, "create", "app.bsky.feed.post", oracleAccountStatusRkey)
	require.NoErrorf(t, err, "account-status lifecycle create: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	for _, status := range oracleNonDeletedAccountStatuses {
		_, err := w.GenerateAccountStatusForTest(t.Context(), oracleAccountStatusLifecycleIndex, false, status)
		require.NoErrorf(t, err, "account-status lifecycle status=%s mode=%s seed=%d", status, cfg.Mode, cfg.Seed)
	}
	_, err = w.GenerateAccountStatusForTest(t.Context(), oracleAccountStatusLifecycleIndex, true, "")
	require.NoErrorf(t, err, "account-status lifecycle reactivation: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	return string(acct.DID)
}

func assertAccountStatusLifecycleArchived(t *testing.T, cfg Config, steady *eventLogRecorder, did string) {
	t.Helper()

	statusCounts := make(map[string]int, len(oracleNonDeletedAccountStatuses))
	var sawProbeCreate bool
	var sawReactivation bool
	for _, ev := range steady.snapshotEvents() {
		if ev.DID != did {
			continue
		}
		if ev.Kind == segment.KindCreate && ev.Collection == "app.bsky.feed.post" && ev.Rkey == oracleAccountStatusRkey {
			sawProbeCreate = true
			continue
		}
		if ev.Kind != segment.KindAccount {
			continue
		}
		var acc comatproto.SyncSubscribeRepos_Account
		require.NoErrorf(t, acc.UnmarshalCBOR(ev.Payload),
			"decode archived account status did=%s seq=%d mode=%s seed=%d", did, ev.Seq, cfg.Mode, cfg.Seed)
		deleted, err := oracleAccountDeleted(ev.Payload)
		require.NoError(t, err)
		require.Falsef(t, deleted,
			"non-deleted account status archived as tombstone: did=%s status=%v mode=%s seed=%d",
			did, acc.Status, cfg.Mode, cfg.Seed)
		if acc.Active && !acc.Status.HasVal() {
			sawReactivation = true
			continue
		}
		if !acc.Active && acc.Status.HasVal() {
			statusCounts[acc.Status.Val()]++
		}
	}

	require.Truef(t, sawProbeCreate,
		"account-status lifecycle probe create did=%s was not archived: mode=%s seed=%d", did, cfg.Mode, cfg.Seed)
	for _, status := range oracleNonDeletedAccountStatuses {
		require.Positivef(t, statusCounts[status],
			"account-status lifecycle status=%s did=%s was not archived: mode=%s seed=%d",
			status, did, cfg.Mode, cfg.Seed)
	}
	require.Truef(t, sawReactivation,
		"account-status lifecycle active=true reactivation did=%s was not archived: mode=%s seed=%d",
		did, cfg.Mode, cfg.Seed)
}

func assertUnavailableRepoStatuses(t *testing.T, dataDir string, w *world.World, cfg Config) {
	t.Helper()

	st, err := metastore.Open(dataDir, nil)
	require.NoErrorf(t, err, "open metadata store for unavailable repo assertions: mode=%s seed=%d", cfg.Mode, cfg.Seed)
	defer func() { require.NoError(t, st.Close()) }()

	bs := backfill.NewStore(st, nil)
	for _, tc := range oracleUnavailableRepos {
		acct, err := w.LoadAccount(tc.accountIdx)
		require.NoError(t, err)
		did := atmos.DID(acct.DID)
		rs, ok, err := backfill.LoadRepoStatus(st, did)
		require.NoErrorf(t, err, "load repo status for unavailable DID %s", did)
		require.Truef(t, ok, "missing repo status for unavailable DID %s", did)
		require.Equalf(t, backfill.StatusUnavailable, rs.Backfill.Status,
			"unavailable DID %s status=%s must persist as StatusUnavailable", did, tc.status)
		require.Truef(t, rs.Active, "unavailable DID %s must retain listRepos active=true so getRepo owns terminal classification", did)
		require.Emptyf(t, rs.Backfill.LastError, "unavailable DID %s must not retain a stale failure error", did)
		require.Zerof(t, rs.Backfill.Attempts, "unavailable DID %s must not count as retry-consuming failure", did)
		require.Zerof(t, rs.Backfill.RetryCount, "unavailable DID %s must not be retry-eligible", did)
		require.Truef(t, rs.Backfill.NextAttemptAt.IsZero(), "unavailable DID %s must not schedule retry", did)

		got, err := bs.Lookup(context.Background(), did)
		require.NoError(t, err)
		require.Equalf(t, atmosbackfill.StateComplete, got.State,
			"unavailable DID %s must project to complete so atmos does not retry", did)
		require.Truef(t, got.Active, "unavailable DID %s lookup must preserve active=true", did)
	}

	counts, ok, err := backfill.LoadCounts(st)
	require.NoError(t, err)
	require.True(t, ok, "backfill counts row missing")
	require.Equalf(t, uint64(len(oracleUnavailableRepos)), counts.Unavailable,
		"unavailable aggregate count mismatch: mode=%s seed=%d", cfg.Mode, cfg.Seed)

	hosts, err := backfill.ListHostStatuses(st)
	require.NoError(t, err)
	var hostUnavailable uint64
	for _, hs := range hosts {
		hostUnavailable += hs.Unavailable
		if hs.Unavailable == 0 {
			continue
		}
		require.Emptyf(t, hs.LatestError, "unavailable host %s must not retain latest failure error", hs.Host)
		require.Emptyf(t, hs.RecentErrors, "unavailable host %s must not retain recent failure samples", hs.Host)
	}
	require.Equalf(t, uint64(len(oracleUnavailableRepos)), hostUnavailable,
		"host unavailable aggregate mismatch: mode=%s seed=%d", cfg.Mode, cfg.Seed)
}

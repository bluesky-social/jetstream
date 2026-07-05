package world

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestDeriveAccount_Deterministic(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 7)
	require.NoError(t, err)
	b, err := deriveAccount(42, 7)
	require.NoError(t, err)
	require.Equal(t, a.DID, b.DID)
	require.Equal(t, a.PrivKeyBytes, b.PrivKeyBytes)
}

func TestDeriveAccount_DifferentInputsDiffer(t *testing.T) {
	t.Parallel()
	a1, _ := deriveAccount(42, 1)
	a2, _ := deriveAccount(42, 2)
	a3, _ := deriveAccount(43, 1)
	require.NotEqual(t, a1.DID, a2.DID)
	require.NotEqual(t, a1.DID, a3.DID)
}

func TestDeriveAccount_DIDIsValid(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 0)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(a.DID), "did:plc:"))
	require.Len(t, a.DID.Identifier(), 24)
	_, err = atmos.ParseDID(string(a.DID))
	require.NoError(t, err)
}

func TestGenerateAccountDeleteForTestMarksInactiveAndEmitsFrame(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(16)))

	frame, err := w.GenerateAccountDeleteForTest(context.Background(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, frame)

	deleted, err := w.IsAccountDeleted(0)
	require.NoError(t, err)
	require.True(t, deleted)

	entries, _, err := w.ListReposPage(0, 10)
	require.NoError(t, err)
	require.False(t, entries[0].Active)
	require.True(t, entries[1].Active)

	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, frame, frames[0])
}

func TestGenerateAccountReactivateForTest_ClearsDeletedAndEmitsActiveFrame(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(16)))

	_, err = w.GenerateAccountDeleteForTest(context.Background(), 0)
	require.NoError(t, err)
	deleted, err := w.IsAccountDeleted(0)
	require.NoError(t, err)
	require.True(t, deleted)

	// Reactivating a non-deleted account is an error.
	_, err = w.GenerateAccountReactivateForTest(context.Background(), 1)
	require.Error(t, err, "reactivating a live account must fail")

	frame, err := w.GenerateAccountReactivateForTest(context.Background(), 0)
	require.NoError(t, err)
	require.NotEmpty(t, frame)

	deleted, err = w.IsAccountDeleted(0)
	require.NoError(t, err)
	require.False(t, deleted, "reactivate must clear the deleted flag")

	acct := decodeAccountFrameForTest(t, frame)
	require.True(t, acct.Active, "reactivate frame must be Active:true")
	require.False(t, acct.Status.HasVal(), "reactivate frame carries no status")

	// Reactivation resets the repo to empty (atproto semantics; matches
	// jetstream's DID-tombstone reset). Account 0 had an initial record.
	rp, _, err := w.LoadRepo(0)
	require.NoError(t, err)
	n := 0
	require.NoError(t, rp.Tree.Walk(func(string, cbor.CID) error { n++; return nil }))
	require.Zero(t, n, "reactivated repo must start empty")

	// Commits are re-enabled: a fresh op on the reactivated account
	// succeeds and is the only record at head.
	_, op, err := w.GenerateRecordOpForTest(context.Background(), 0, "create", collPost, "3kreactivepost0")
	require.NoError(t, err)
	require.Equal(t, "create", op.Action)

	rp, _, err = w.LoadRepo(0)
	require.NoError(t, err)
	n = 0
	require.NoError(t, rp.Tree.Walk(func(string, cbor.CID) error { n++; return nil }))
	require.Equal(t, 1, n, "only the post-reactivation record is live")

	entries, _, err := w.ListReposPage(0, 10)
	require.NoError(t, err)
	require.True(t, entries[0].Active, "reactivated account must list as active")
}

func TestGenerateAccountStatusForTest_NonDeletedStatusesDoNotMarkDeleted(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(16)))

	for _, status := range []string{"takendown", "suspended", "deactivated", "unknown"} {
		frame, err := w.GenerateAccountStatusForTest(context.Background(), 0, false, status)
		require.NoError(t, err)
		acct := decodeAccountFrameForTest(t, frame)
		require.False(t, acct.Active)
		require.Equal(t, status, acct.Status.Val())

		deleted, err := w.IsAccountDeleted(0)
		require.NoError(t, err)
		require.Falsef(t, deleted, "status %q must not mark the world account deleted", status)
	}

	frame, err := w.GenerateAccountStatusForTest(context.Background(), 0, true, "")
	require.NoError(t, err)
	acct := decodeAccountFrameForTest(t, frame)
	require.True(t, acct.Active)
	require.False(t, acct.Status.HasVal())

	entries, _, err := w.ListReposPage(0, 10)
	require.NoError(t, err)
	require.True(t, entries[0].Active, "non-deleted account statuses must not flip listRepos active")
}

func TestSetRepoUnavailableForTest_MarksListReposInactive(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Accounts = 2
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))

	require.NoError(t, w.SetRepoUnavailableForTest(1, "suspended"))
	status, ok, err := w.RepoUnavailableStatus(1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "suspended", status)

	entries, _, err := w.ListReposPage(0, 10)
	require.NoError(t, err)
	require.True(t, entries[0].Active)
	require.False(t, entries[1].Active, "unavailable repo must list inactive")
	require.Error(t, w.SetRepoUnavailableForTest(1, "unknown"))
}

func TestGenerateAccountDeleteForTestUsesDeterministicTime(t *testing.T) {
	t.Parallel()

	firstFrame, firstAccount := generateAccountDeleteFrameForSeed(t, 99)
	time.Sleep(3 * time.Millisecond)
	secondFrame, secondAccount := generateAccountDeleteFrameForSeed(t, 99)

	require.Equal(t, firstFrame, secondFrame, "same-seed account delete frames must be byte-identical")
	require.Equal(t, firstAccount.Time, secondAccount.Time)
	require.Equal(t, "2023-11-14T22:13:20.000Z", firstAccount.Time)
}

func generateAccountDeleteFrameForSeed(t *testing.T, seed uint64) ([]byte, comatproto.SyncSubscribeRepos_Account) {
	t.Helper()

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Seed = seed
	cfg.Accounts = 2
	cfg.InitialRecords = 1

	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(1, 2)), fanout.New(16)))

	frame, err := w.GenerateAccountDeleteForTest(context.Background(), 0)
	require.NoError(t, err)
	return frame, decodeAccountFrameForTest(t, frame)
}

func decodeAccountFrameForTest(t *testing.T, frame []byte) comatproto.SyncSubscribeRepos_Account {
	t.Helper()

	count, pos, err := cbor.ReadMapHeader(frame, 0)
	require.NoError(t, err)

	var typ string
	for range count {
		key, next, err := cbor.ReadText(frame, pos)
		require.NoError(t, err)
		pos = next
		switch key {
		case "op":
			var op int64
			op, pos, err = cbor.ReadInt(frame, pos)
			require.NoError(t, err)
			require.Equal(t, int64(1), op)
		case "t":
			typ, pos, err = cbor.ReadText(frame, pos)
			require.NoError(t, err)
		default:
			pos, err = cbor.SkipValue(frame, pos)
			require.NoError(t, err)
		}
	}
	require.Equal(t, "#account", typ)

	var account comatproto.SyncSubscribeRepos_Account
	require.NoError(t, account.UnmarshalCBOR(frame[pos:]))
	return account
}

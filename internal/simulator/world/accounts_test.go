package world

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream-v2/internal/simulator/fanout"
	"github.com/jcalabro/atmos"
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

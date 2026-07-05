package world

import (
	"bytes"
	"context"
	"log/slog"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/bluesky-social/jetstream/internal/simulator/fanout"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/stretchr/testify/require"
)

func decodeIdentityFrame(t *testing.T, frame []byte) comatproto.SyncSubscribeRepos_Identity {
	t.Helper()
	body, ok := bytes.CutPrefix(frame, frameHeaderIdentity)
	require.True(t, ok, "expected #identity header")
	var evt comatproto.SyncSubscribeRepos_Identity
	require.NoError(t, evt.UnmarshalCBOR(body))
	return evt
}

func TestGenerateIdentityForTest_HandleAbsent(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.GenerateIdentityForTest(t.Context(), 0, false)
	require.NoError(t, err)

	evt := decodeIdentityFrame(t, frame)
	require.Equal(t, int64(1), evt.Seq)
	require.False(t, evt.Handle.HasVal(), "handle-absent variant must omit the handle")
	require.NotEmpty(t, evt.Time)

	a, err := w.loadAccount(0)
	require.NoError(t, err)
	require.Equal(t, string(a.DID), evt.DID)
	require.NoError(t, atmos.DID(evt.DID).Validate())

	// The frame is persisted to firehose history at its seq.
	frames, err := w.FirehoseRange(0, 10)
	require.NoError(t, err)
	require.Len(t, frames, 1)
	require.Equal(t, frame, frames[0])
}

func TestGenerateIdentityForTest_HandleChangesAreDistinctAndPersistent(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	first, err := w.GenerateIdentityForTest(t.Context(), 3, true)
	require.NoError(t, err)
	second, err := w.GenerateIdentityForTest(t.Context(), 3, true)
	require.NoError(t, err)

	evt1 := decodeIdentityFrame(t, first)
	evt2 := decodeIdentityFrame(t, second)
	require.True(t, evt1.Handle.HasVal())
	require.True(t, evt2.Handle.HasVal())
	require.Equal(t, "user-3-h1.test", evt1.Handle.Val())
	require.Equal(t, "user-3-h2.test", evt2.Handle.Val(),
		"persisted change counter must make successive handles distinct")
	require.Equal(t, evt1.DID, evt2.DID)
	require.Greater(t, evt2.Seq, evt1.Seq)
}

func TestGenerateMalformedIdentityForTest_DIDFailsValidation(t *testing.T) {
	t.Parallel()
	w := newTestWorld(t)

	frame, err := w.GenerateMalformedIdentityForTest(t.Context())
	require.NoError(t, err)

	evt := decodeIdentityFrame(t, frame)
	require.Equal(t, MalformedIdentityDID, evt.DID)
	require.Error(t, atmos.DID(evt.DID).Validate(),
		"the malformed variant exists to trip DID validation; a passing DID makes the oracle case vacuous")
}

// TestGenerateOne_DefaultMixEmitsIdentityFrames pins the anti-vacuity
// property of the default mix itself: under DefaultTrafficMix a
// realistic run length must produce at least one #identity frame, or
// the ingest path is dead under every default-config oracle tier
// (the m013/m014 green-by-vacancy lesson).
func TestGenerateOne_DefaultMixEmitsIdentityFrames(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.Accounts = 5
	cfg.InitialRecords = 1
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	_, err = w.EnsureSeed()
	require.NoError(t, err)
	require.NoError(t, w.Bootstrap(context.Background(), slog.Default()))
	require.NoError(t, w.AttachRuntime(rand.New(rand.NewPCG(11, 22)), fanout.New(64)))

	var identities, commits int
	for range 200 {
		frame, err := w.generateOne(context.Background())
		require.NoError(t, err)
		switch {
		case bytes.HasPrefix(frame, frameHeaderIdentity):
			identities++
			evt := decodeIdentityFrame(t, frame)
			require.NoError(t, atmos.DID(evt.DID).Validate(),
				"random-mix identity frames must stay polite; malformed DIDs are injection-only")
		case bytes.HasPrefix(frame, frameHeaderCommit):
			commits++
		default:
			t.Fatalf("unexpected frame kind (header bytes %x)", frame[:min(8, len(frame))])
		}
	}
	require.NotZero(t, identities, "default mix produced no #identity frames in 200 draws")
	require.Greater(t, commits, identities, "commits must dominate the default mix")
}

func TestTrafficMix_ZeroValueDefaultsAndValidation(t *testing.T) {
	t.Parallel()

	// Zero-value mix defaults to DefaultTrafficMix at construction.
	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "simulator")
	cfg.TrafficMix = TrafficMix{}
	w, err := New(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	require.NotEmpty(t, w.kindMix)
	require.NotEmpty(t, w.actionMix)

	// Negative weights are rejected loudly.
	bad := DefaultConfig()
	bad.DataDir = filepath.Join(t.TempDir(), "simulator-bad")
	bad.TrafficMix = TrafficMix{Create: -1}
	_, err = New(context.Background(), bad)
	require.ErrorContains(t, err, "TrafficMix.Create")
}

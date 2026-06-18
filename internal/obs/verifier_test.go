package obs_test

import (
	"errors"
	"testing"

	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestClassify_FallbackOther(t *testing.T) {
	t.Parallel()
	require.Equal(t, "other", obs.Classify(errors.New("totally unrecognized")))
}

func TestClassify_NilIsOther(t *testing.T) {
	t.Parallel()
	// A nil error reaching Classify is itself a caller bug; we map to
	// "other" rather than panic so a single misuse can't crash the
	// process.
	require.Equal(t, "other", obs.Classify(nil))
}

func TestClassify_Signature(t *testing.T) {
	t.Parallel()
	require.Equal(t, "signature", obs.Classify(errors.New("signature verification failed")))
}

func TestClassify_Chain(t *testing.T) {
	t.Parallel()
	require.Equal(t, "chain", obs.Classify(errors.New("chain state mismatch")))
}

func TestClassify_Hosting(t *testing.T) {
	t.Parallel()
	require.Equal(t, "hosting", obs.Classify(errors.New("hosting state invalid")))
}

func TestClassify_Resolve(t *testing.T) {
	t.Parallel()
	require.Equal(t, "resolve", obs.Classify(errors.New("could not resolve did")))
}

// TestClassify_LowercasesBeforeMatch locks in the case-insensitivity
// contract: callers may pass errors from atmos that were originally
// formatted with capitalized words ("Signature mismatch") and we still
// classify them correctly.
func TestClassify_LowercasesBeforeMatch(t *testing.T) {
	t.Parallel()
	require.Equal(t, "signature", obs.Classify(errors.New("Signature MISMATCH")))
}

// TestClassify_PrecedenceOrder pins the documented ordering: when an
// error message contains keywords from multiple kinds, the earliest
// case in the switch wins. This locks the precedence so accidental
// reordering is caught.
func TestClassify_PrecedenceOrder(t *testing.T) {
	t.Parallel()
	require.Equal(t, "signature",
		obs.Classify(errors.New("invalid signature inside chain walk")))
	require.Equal(t, "chain",
		obs.Classify(errors.New("chain validation failed during hosting check")))
	require.Equal(t, "hosting",
		obs.Classify(errors.New("could not resolve hosting endpoint")))
}

func TestVerifierMetrics_NilSafeAndIncrement(t *testing.T) {
	t.Parallel()
	// nil receiver must be a no-op (matches the codebase convention).
	var m *obs.VerifierMetrics
	m.IncFailure("signature")

	reg := prometheus.NewRegistry()
	m = obs.NewVerifierMetrics(reg)

	m.IncFailure("signature")
	m.IncFailure("signature")
	m.IncFailure("chain")

	require.Equal(t, float64(2), testutil.ToFloat64(m.Failures.WithLabelValues("signature")))
	require.Equal(t, float64(1), testutil.ToFloat64(m.Failures.WithLabelValues("chain")))
}

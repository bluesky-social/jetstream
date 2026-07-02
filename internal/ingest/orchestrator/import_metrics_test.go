package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestIsCancellationOnly is the shared pause-vs-fail classifier both the import
// manager (importer.run) and the import metrics (observeJob) key on. The
// load-bearing case is a joined error: errors.Is(context.Canceled) matches any
// leaf, so a real failure joined with a shutdown cancellation must NOT read as
// cancellation-only, or it would be laundered into a resumable pause and
// dropped from jobs_total.
func TestIsCancellationOnly(t *testing.T) {
	t.Parallel()
	realErr := errors.New("manifest refresh failed")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not cancellation", nil, false},
		{"bare Canceled", context.Canceled, true},
		{"bare DeadlineExceeded", context.DeadlineExceeded, true},
		{"wrapped Canceled", fmt.Errorf("parse/bucket: %w", context.Canceled), true},
		{"doubly wrapped Canceled", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled)), true},
		{"plain error", realErr, false},
		{"join of two cancellations", errors.Join(context.Canceled, context.DeadlineExceeded), true},
		{"join Canceled + real failure", errors.Join(context.Canceled, realErr), false},
		{"join real failure + Canceled", errors.Join(realErr, context.Canceled), false},
		{"wrapped join Canceled + real", fmt.Errorf("apply: %w", errors.Join(context.Canceled, realErr)), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, IsCancellationOnly(tt.err))
		})
	}
}

// TestObserveJob_JoinedCancellationCountsAsTerminal pins the fix that motivated
// sharing the classifier: a job that FAILED while a shutdown cancellation raced
// it (errors.Join(Canceled, realFailure)) must still increment
// jobs_total{result="error"} — a bare errors.Is(Canceled) would have dropped it,
// hiding exactly the failure the alert exists to catch.
func TestObserveJob_JoinedCancellationCountsAsTerminal(t *testing.T) {
	t.Parallel()
	im := NewImportMetrics(prometheus.NewRegistry())
	joined := errors.Join(context.Canceled, errors.New("manifest refresh failed"))
	im.observeJob(time.Now(), ImportResult{}, joined)
	require.EqualValues(t, 1, testutil.ToFloat64(im.Jobs.WithLabelValues("error")),
		"a real failure joined with cancellation is a terminal error")

	// A pure (joined) cancellation is still a pause: not counted.
	im2 := NewImportMetrics(prometheus.NewRegistry())
	im2.observeJob(time.Now(), ImportResult{}, errors.Join(context.Canceled, context.DeadlineExceeded))
	require.EqualValues(t, 0, testutil.ToFloat64(im2.Jobs.WithLabelValues("error")),
		"pure cancellation is a pause, not a terminal error")
}

package segment_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/stretchr/testify/require"
)

// fakeSealObserver is a dependency-free SealObserver for exercising the
// segment metrics seam without pulling Prometheus into the decode/seal core.
type fakeSealObserver struct {
	calls int
	errs  int
}

func (f *fakeSealObserver) ObserveSeal(_ time.Time, err error) {
	f.calls++
	if err != nil {
		f.errs++
	}
}

// TestSeal_InvokesObserver drives a real Writer through Seal with a
// configured observer and confirms exactly one successful observation.
func TestSeal_InvokesObserver(t *testing.T) {
	t.Parallel()
	obs := &fakeSealObserver{}

	path := filepath.Join(t.TempDir(), "seg_0.jss")
	w, err := segment.New(segment.Config{
		Path:    path,
		Metrics: obs,
	})
	require.NoError(t, err)

	_, err = w.Append(segment.Event{
		IndexedAt: 1,
		Kind:      segment.KindCreate,
		DID:       "did:plc:test",
		Seq:       1,
	})
	require.NoError(t, err)

	_, err = w.Seal()
	require.NoError(t, err)

	require.Equal(t, 1, obs.calls)
	require.Equal(t, 0, obs.errs)
}

package jetstreamd

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/storagefake"
)

type failingReads struct {
	metastore.Store
	err error
}

func (s failingReads) Get(context.Context, []byte) ([]byte, error) { return nil, s.err }

func (s failingReads) NewIter(context.Context, []byte, []byte) (metastore.Iterator, error) {
	return nil, s.err
}

// A failed metadata read in a leader session restarts the session instead
// of reaching the leader loop as a fatal error. A missing key and a
// cancelled context are not failures.
func TestSessionStore_ReadFailureEndsSession(t *testing.T) {
	t.Parallel()
	newStore := func(err error) (*sessionStore, *catalog.Session) {
		sess := catalog.NewSession(catalog.SessionConfig{DB: storagefake.New(storagefake.Config{}), Epoch: 1})
		return &sessionStore{Store: failingReads{Store: memstore.New(), err: err}, sess: sess}, sess
	}

	s, sess := newStore(metastore.ErrNotFound)
	_, err := s.Get(t.Context(), []byte("k"))
	require.ErrorIs(t, err, metastore.ErrNotFound)
	require.NoError(t, sess.Err())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s, sess = newStore(context.Canceled)
	_, err = s.Get(ctx, []byte("k"))
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, sess.Err())

	conn := errors.New("connection reset")
	for name, read := range map[string]func(*sessionStore) error{
		"get":  func(s *sessionStore) error { _, err := s.Get(t.Context(), []byte("k")); return err },
		"iter": func(s *sessionStore) error { _, err := s.NewIter(t.Context(), nil, nil); return err },
	} {
		s, sess := newStore(conn)
		err := read(s)
		require.ErrorIs(t, err, conn, name)
		require.ErrorIs(t, err, leader.ErrRestartSession, name)
		require.ErrorIs(t, sess.Err(), catalog.ErrSessionEnded, name)
		require.False(t, leader.DefaultFatal(sessionError(errors.New("fallout"), sess)), name)
	}
}

// An object store that stayed unreachable past its retry budget restarts the
// session instead of exiting the process (design §16), unless the error is
// also corruption. This is the S3 outage from the disaggregated test-bed
// findings, where a compaction rebuild's failed GET was fatal.
func TestSessionError_ObjectStoreUnavailableRestarts(t *testing.T) {
	t.Parallel()
	sess := catalog.NewSession(catalog.SessionConfig{DB: storagefake.New(storagefake.Config{}), Epoch: 1})
	outage := fmt.Errorf("orchestrator: compaction: rebuild: s3: get %q: %w: gave up after 3 attempts", "k", objstore.ErrUnavailable)

	err := sessionError(outage, sess)
	require.ErrorIs(t, err, objstore.ErrUnavailable)
	require.ErrorIs(t, err, leader.ErrRestartSession)
	require.False(t, leader.DefaultFatal(err))

	require.True(t, leader.DefaultFatal(sessionError(errors.New("some other failure"), sess)))

	corrupt := &catalog.CorruptionError{Source: catalog.SourceRead, Err: outage}
	require.True(t, leader.DefaultFatal(sessionError(corrupt, sess)), "corruption stays fatal")
}

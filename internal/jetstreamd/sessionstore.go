package jetstreamd

import (
	"context"
	"errors"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
)

// sessionStore is a leader session's metadata store in disaggregated mode.
// Writes already end the session when they fail, because each is a fenced
// catalog transaction. Reads are autocommit queries outside the session, so
// without this a dropped PostgreSQL connection during a read would reach
// the leader loop as an unclassified, fatal error. Ending the session
// instead makes it a restart (design §9.1): the next session re-reads
// everything from durable state.
type sessionStore struct {
	metastore.Store
	sess *catalog.Session
}

func (s *sessionStore) Unwrap() metastore.Store { return s.Store }

func (s *sessionStore) end(ctx context.Context, op string, err error) error {
	if err == nil || errors.Is(err, metastore.ErrNotFound) || ctx.Err() != nil {
		return err
	}
	return s.sess.End(op, err)
}

func (s *sessionStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	v, err := s.Store.Get(ctx, key)
	return v, s.end(ctx, "metadata read", err)
}

func (s *sessionStore) NewIter(ctx context.Context, lower, upper []byte) (metastore.Iterator, error) {
	it, err := s.Store.NewIter(ctx, lower, upper)
	if err != nil {
		return nil, s.end(ctx, "metadata iterate", err)
	}
	return &sessionIter{Iterator: it, ctx: ctx, s: s}, nil
}

type sessionIter struct {
	metastore.Iterator
	ctx context.Context
	s   *sessionStore
}

func (it *sessionIter) Err() error {
	return it.s.end(it.ctx, "metadata iterate", it.Iterator.Err())
}

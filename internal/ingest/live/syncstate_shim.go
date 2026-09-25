package live

import (
	"context"
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/cockroachdb/pebble"
)

// stageSyncState stages ss's promoted state into the writer's durable batch.
// Transitional (S1.3): syncstate takes a metastore.Batch, but the writer's
// durable-batch hook still hands this package a *pebble.Batch. Delete this
// file when live moves onto metastore.
func stageSyncState(ss *syncstate.StateStore, b *pebble.Batch) error {
	sb := &pebbleStageBatch{b: b}
	ss.StageFlush(sb)
	if sb.err != nil {
		return fmt.Errorf("livestream: stage sync state: %w", sb.err)
	}
	return nil
}

// pebbleStageBatch stages onto a batch the writer commits, so it never
// commits itself. Pebble only fails staging on a closed batch; the first
// error is latched for stageSyncState.
type pebbleStageBatch struct {
	b   *pebble.Batch
	err error
}

var _ metastore.Batch = (*pebbleStageBatch)(nil)

func (s *pebbleStageBatch) Set(key, value []byte) {
	if s.err == nil {
		s.err = s.b.Set(key, value, nil)
	}
}

func (s *pebbleStageBatch) Delete(key []byte) {
	if s.err == nil {
		s.err = s.b.Delete(key, nil)
	}
}

func (s *pebbleStageBatch) DeleteRange(start, end []byte) {
	if s.err == nil {
		s.err = s.b.DeleteRange(start, end, nil)
	}
}

func (s *pebbleStageBatch) Len() int { return int(s.b.Count()) }

func (s *pebbleStageBatch) Commit(context.Context) error {
	return errors.New("livestream: sync state staging batch is committed by the writer")
}

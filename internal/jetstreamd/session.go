package jetstreamd

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/orchestrator"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/internal/subscribe"
	"github.com/bluesky-social/jetstream/internal/tombstone"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/gt"
)

// A writer session is one run of the ingest lifecycle under the writer lock
// (design §6.3). Everything that writes, or caches state derived from
// writes, is built per session so a new session starts from durable state
// alone. Everything that serves reads is built once per process and survives
// session changes through writerSlot and compactionDeadline.

// writerSession holds one session's write-side state.
type writerSession struct {
	orch       *orchestrator.Orchestrator
	verifier   *atmossync.Verifier
	tombstones *tombstone.Set
	schedule   *orchestrator.CompactionScheduleState
}

// sessionFactory builds writer sessions from process-lifetime dependencies.
type sessionFactory struct {
	// store is the metadata write handle. Local mode has one writer per data
	// directory, so it is the process handle; a shared backend fences it by
	// epoch instead.
	store           metastore.Store
	directory       *identity.Directory
	syncClient      *atmossync.Client
	verifierMetrics *obs.VerifierMetrics
	logger          *slog.Logger

	// orch is the orchestrator config with every process-lifetime field set.
	// build fills in the per-session ones.
	orch orchestrator.Config
}

func (f *sessionFactory) build() (*writerSession, error) {
	return f.buildWith(f.store, nil)
}

// buildWith builds a session over store. A non-nil hot runs the
// orchestrator's writer in hot mode (disaggregated storage), where store is
// the session's epoch-fenced handle.
func (f *sessionFactory) buildWith(store metastore.Store, hot *ingest.HotConfig) (*writerSession, error) {
	stateStore := syncstate.New(store)
	tombstones := tombstone.New()
	// A new session's schedule starts unknown, so archive responses are not
	// cached until its compactor has scheduled a pass.
	schedule := orchestrator.NewCompactionScheduleState()

	verifierLogger := f.logger.With(slog.String("component", "verifier"))
	verifier, err := atmossync.NewVerifier(atmossync.VerifierOptions{
		Directory:  f.directory,
		StateStore: stateStore,
		SyncClient: gt.Some(f.syncClient),
		OnVerificationFailure: gt.Some(func(did atmos.DID, vErr error) {
			f.verifierMetrics.IncFailure(obs.Classify(vErr))
			verifierLogger.Warn("verification failure",
				"did", did,
				"err", vErr,
			)
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("serve: build verifier: %w", err)
	}

	cfg := f.orch
	cfg.Store = store
	cfg.Hot = hot
	cfg.Verifier = verifier
	cfg.SyncStateStore = stateStore
	cfg.Tombstones = tombstones
	cfg.CompactionSchedule = schedule
	orch, err := orchestrator.New(cfg)
	if err != nil {
		_ = verifier.Close()
		return nil, fmt.Errorf("serve: build orchestrator: %w", err)
	}
	return &writerSession{
		orch:       orch,
		verifier:   verifier,
		tombstones: tombstones,
		schedule:   schedule,
	}, nil
}

// runSession is the leader.SessionFunc. It returns only after the
// orchestrator and every session goroutine have stopped and the verifier is
// closed, so per-session state is gone before the next session starts or the
// metadata store closes.
func (r *Runtime) runSession(ctx context.Context, epoch uint64) error {
	s, err := r.takeSession()
	if err != nil {
		return err
	}
	return r.runWriterSession(ctx, epoch, s)
}

// runWriterSession runs s to completion. It returns only after the
// orchestrator and every session goroutine have stopped and the verifier is
// closed.
func (r *Runtime) runWriterSession(ctx context.Context, epoch uint64, s *writerSession) error {
	r.orchMetrics.SetTombstones(s.tombstones)
	r.deadline.set(s.schedule)
	defer func() {
		r.deadline.set(nil)
		r.orchMetrics.SetTombstones(nil)
	}()

	if r.opts.OnSessionStart != nil {
		r.opts.OnSessionStart(epoch)
	}

	// Verification failures are diagnostic, not fatal: they typically
	// reflect adversarial or malformed PDS input, which is invalid user
	// data, not a jetstream bug. OnVerificationFailure feeds the metric;
	// this only logs.
	verifierLogger := r.processLogger.With(slog.String("component", "verifier"))
	stopDrain := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		_ = r.goroutineRoot("verifier_async_errors", func() error {
			for {
				select {
				case <-stopDrain:
					return nil
				case err, ok := <-s.verifier.AsyncErrors():
					if !ok {
						return nil
					}
					verifierLogger.Warn("async error", "err", err)
				}
			}
		})()
	}()

	err := s.orch.Run(ctx)

	close(stopDrain)
	<-drainDone
	// Promoted sync state is flushed by the live consumer's own Close, after
	// its writer fsyncs every appended row, which has already happened
	// inside Run. Pending entries are deliberately dropped: their rows were
	// never archived and redelivery re-verifies them.
	if cerr := s.verifier.Close(); cerr != nil {
		r.logger.Error("verifier close", "epoch", epoch, "err", cerr)
	}
	return err
}

// takeSession returns the session Build constructed for validation, or a new
// one once that has been used.
func (r *Runtime) takeSession() (*writerSession, error) {
	r.closeMu.Lock()
	s := r.pending
	r.pending = nil
	r.closeMu.Unlock()
	if s != nil {
		return s, nil
	}
	return r.sessions.build()
}

// writerSlot publishes the current session's steady-state writer to the
// per-process readers: the subscribe tail, the cold reader, cursor
// resolution, status, and repo actions.
//
// The slot keeps the last writer after its session ends, until the next
// session publishes. Clearing it would let a live subscriber that passed the
// handler's writer check anchor at Tip() 0 and then replay the whole
// archive. The stale writer is closed, so its log serves nothing new, and
// the seq lease guarantees the next writer's seqs start past its tip.
type writerSlot struct {
	ptr  atomic.Pointer[ingest.Writer]
	tail *subscribe.Tail
}

func (s *writerSlot) readLog() *ingest.ReadableLog {
	if w := s.ptr.Load(); w != nil {
		return w.ReadLog()
	}
	return nil
}

func (s *writerSlot) nextSeq() uint64 {
	if w := s.ptr.Load(); w != nil {
		return w.NextSeq()
	}
	return 0
}

// publish fires after the steady writer opens and before any producer (live
// consumer, retry runner, compactor) starts, so subscribers read the
// writer-owned log from its first event.
func (s *writerSlot) publish(w *ingest.Writer) {
	s.ptr.Store(w)
	// Re-installing the source wakes readers parked on the previous
	// session's log.
	s.tail.SetReadLogSource(s.readLog)
}

// compactionDeadline serves the current session's compaction schedule to
// xrpcapi, which outlives sessions. Between sessions the schedule is
// unknown, so responses are not cached.
type compactionDeadline struct {
	cur atomic.Pointer[orchestrator.CompactionScheduleState]
}

func (d *compactionDeadline) set(s *orchestrator.CompactionScheduleState) { d.cur.Store(s) }

// NextCompactionAt implements xrpcapi.CompactionDeadline.
func (d *compactionDeadline) NextCompactionAt() (time.Time, bool) {
	return d.cur.Load().NextCompactionAt()
}

package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/store"
	"github.com/cockroachdb/pebble"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/prometheus/client_golang/prometheus"
)

// listReposPageLimit matches the value the atmos backfill engine
// uses (and the protocol maximum). Pulled out as a constant so the
// metric labels and any future tuning have a single source of truth.
const listReposPageLimit = 1000

// seedBatchSize is the number of new DIDs we batch into a single
// pebble write before fsync. The metadata store is several million
// keys; per-row Sync writes are dominated by fsync latency, so
// batching gives a roughly N× speedup at the cost of losing the last
// (incomplete) batch on crash. That's fine: the seed step is fully
// idempotent and a re-run picks up where the previous one stopped.
const seedBatchSize = 1000

// Metrics groups the prometheus collectors the seed step emits.
// Owned by the caller so multiple bootstrap runs in the same process
// (e.g. tests) can share or isolate registries.
type Metrics struct {
	// EnumeratedTotal is the count of listRepos entries observed,
	// regardless of whether they triggered a write.
	EnumeratedTotal prometheus.Counter

	// SeededTotal is the count of new repo/<did> rows written. A
	// re-run on an existing data dir will leave this near zero.
	SeededTotal prometheus.Counter

	// SkippedExistingTotal counts entries we observed but did not
	// rewrite because a row already existed. Watching this rise
	// while SeededTotal stays flat is the signal that a re-run is
	// progressing.
	SkippedExistingTotal prometheus.Counter
}

// NewSeedMetrics registers the seed-step counters on reg. The
// counters live under the `jetstream_backfill_*` namespace.
func NewSeedMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		EnumeratedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "backfill",
			Name:      "enumerated_total",
			Help:      "Number of listRepos entries observed during the seed step.",
		}),
		SeededTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "backfill",
			Name:      "seeded_total",
			Help:      "Number of new repo/<did> rows written during the seed step.",
		}),
		SkippedExistingTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream",
			Subsystem: "backfill",
			Name:      "skipped_existing_total",
			Help:      "Number of listRepos entries skipped because a repo/<did> row already existed.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.EnumeratedTotal, m.SeededTotal, m.SkippedExistingTotal)
	}
	return m
}

// SeedReposLister is the subset of atmos sync.Client that SeedRepos
// depends on. Splitting the dependency to an interface lets tests
// substitute a fake without standing up a real XRPC server (the
// real sync.Client only constructs through xrpc.Client).
type SeedReposLister interface {
	ListRepos(ctx context.Context, limit int64) listReposIter
}

// listReposIter is the shape sync.Client.ListRepos returns. It's
// declared here so SeedReposLister doesn't drag a typedef from
// atmos/sync into every test file.
type listReposIter = func(yield func(atmossync.ListReposEntry, error) bool)

// realLister adapts atmos sync.Client to SeedReposLister. Wrapping
// keeps the type alias visible only inside this package.
type realLister struct{ c *atmossync.Client }

func (r realLister) ListRepos(ctx context.Context, limit int64) listReposIter {
	return r.c.ListRepos(ctx, limit)
}

// SeedRepos enumerates every account on the relay via
// com.atproto.sync.listRepos and writes a `repo/<did>` row with
// Status = StatusNotStarted for any DID that does not already have
// one. DIDs that are already present are left untouched, regardless
// of their current Status — re-running this step is the documented
// way to pick up DIDs that have appeared since the last run.
//
// SeedRepos blocks until the relay's listRepos pagination is fully
// drained or ctx is cancelled. On ctx cancellation, any rows that
// were durably batched up to that point are kept (that's the point
// of the batching) and the remainder is reported via the returned
// error. A subsequent call resumes from the relay's authoritative
// listing — we deliberately do not persist a listRepos cursor here,
// because a stale cursor would silently skip DIDs created during
// the gap.
//
// The returned SeedResult counts the work done in this run; the
// metrics collectors on m are the cumulative source of truth across
// process restarts / multiple calls.
func SeedRepos(ctx context.Context, s *store.Store, client *atmossync.Client, m *Metrics, logger *slog.Logger) (SeedResult, error) {
	return seedReposImpl(ctx, s, realLister{c: client}, m, logger)
}

// SeedResult summarizes a single SeedRepos invocation. It is a pure
// summary — the per-call counterpart to the cumulative SeedMetrics.
type SeedResult struct {
	Enumerated      int64
	Seeded          int64
	SkippedExisting int64
}

func seedReposImpl(ctx context.Context, s *store.Store, lister SeedReposLister, met *Metrics, logger *slog.Logger) (SeedResult, error) {
	now := time.Now().UTC()
	if logger == nil {
		logger = slog.Default()
	}

	logger.Info("backfill: seeding repo list from relay", "page_limit", listReposPageLimit)

	batch := s.NewBatch()
	defer func() {
		if err := batch.Close(); err != nil {
			logger.Error("backfill: failed to close batch", "err", err)
		}
	}()

	pendingInBatch := 0
	var res SeedResult

	flush := func() error {
		if pendingInBatch == 0 {
			return nil
		}

		const maxRetries = 3
		for retry := range maxRetries {
			err := batch.Commit(pebble.Sync)
			if err == nil {
				break
			}

			if retry == maxRetries-1 {
				return fmt.Errorf("backfill: commit seed batch: %w", err)
			}

			time.Sleep(250 * time.Millisecond)
		}

		batch.Reset()
		pendingInBatch = 0
		return nil
	}

	for entry, err := range lister.ListRepos(ctx, listReposPageLimit) {
		if ctx.Err() != nil {
			// Best-effort flush of whatever we batched before
			// surfacing the cancellation.
			_ = flush()
			return res, ctx.Err()
		}
		if err != nil {
			_ = flush()
			return res, fmt.Errorf("backfill: listRepos: %w", err)
		}

		res.Enumerated++
		met.EnumeratedTotal.Inc()

		// Skip DIDs we've already recorded
		exists, err := HasRepo(s, entry.DID)
		if err != nil {
			_ = flush()
			return res, err
		}
		if exists {
			res.SkippedExisting++
			met.SkippedExistingTotal.Inc()
			continue
		}

		rs := RepoStatus{
			Backfill: RepoBackfillStatus{
				Status: StatusNotStarted,
			},
			UpdatedAt: now,
		}

		buf, err := json.Marshal(rs)
		if err != nil {
			_ = flush()
			return res, err
		}

		if err := batch.Set(repoKey(entry.DID), buf, nil); err != nil {
			_ = flush()
			return res, fmt.Errorf("backfill: stage seed write: %w", err)
		}

		res.Seeded++
		pendingInBatch++
		met.SeededTotal.Inc()

		if pendingInBatch >= seedBatchSize {
			if err := flush(); err != nil {
				return res, err
			}
		}
	}

	if err := flush(); err != nil {
		return res, err
	}

	logger.Info("backfill: seed complete",
		"enumerated", res.Enumerated,
		"seeded", res.Seeded,
		"skipped_existing", res.SkippedExisting,
	)

	return res, nil
}

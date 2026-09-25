package maintainer

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/bluesky-social/jetstream/segment"
)

// RebuildConfig configures Rebuild.
type RebuildConfig struct {
	// BlockMaxAge must match the hot writer's. Zero means
	// ingest.DefaultBlockMaxAge.
	BlockMaxAge time.Duration
	// RelayCursor checks design §9.3 invariant 7 at session start; see
	// catalog.InvariantOptions. Only a test model can check it, so
	// production passes nil.
	RelayCursor func(s *catalog.Snapshot, value []byte) error
}

// Rebuild is session start in hot mode (design §10.9 steps 2-6). It checks
// the cheap catalog invariants, reads every hot batch, and regroups the
// batches greedily into blocks without splitting one, because the sessions
// that wrote them may have cut blocks elsewhere. Every group but the last
// can no longer grow, so it folds; the last folds only if it is full or
// older than BlockMaxAge. Seals follow the rotation rule as usual.
//
// It returns the unfolded last group as the hot writer's open block
// (ingest.HotConfig.Resume), or nil. Call it once, after Open and before the
// writer opens. A failure ends the session like any other.
func (m *Maintainer) Rebuild(ctx context.Context, cfg RebuildConfig) (*ingest.OpenBlock, error) {
	var open *ingest.OpenBlock
	err := obs.Span(ctx, func(ctx context.Context) error {
		var err error
		open, err = m.rebuild(ctx, cfg)
		return err
	})
	if err != nil {
		m.fail(err)
		return nil, err
	}
	return open, nil
}

// hotBatch is one decoded hot batch row.
type hotBatch struct {
	row    catalog.HotBatchRow
	events []segment.Event
}

func (m *Maintainer) rebuild(ctx context.Context, cfg RebuildConfig) (*ingest.OpenBlock, error) {
	start := time.Now()
	rows, err := m.loadHot(ctx, cfg)
	if err != nil {
		return nil, err
	}
	batches, err := m.decodeHot(ctx, rows)
	if err != nil {
		return nil, err
	}
	groups, err := groupBatches(batches, m.maxEvents)
	if err != nil {
		return nil, err
	}

	fold := groups
	var open *ingest.OpenBlock
	if len(groups) > 0 {
		last := groups[len(groups)-1]
		now := time.Now()
		// CommittedAt is the database's clock; clamp it so skew cannot
		// push the open block's age cut past BlockMaxAge from now.
		openedAt := last[0].row.CommittedAt
		if openedAt.After(now) {
			openedAt = now
		}
		age := cfg.BlockMaxAge
		if age == 0 {
			age = ingest.DefaultBlockMaxAge
		}
		if eventCount(last) < m.maxEvents && now.Before(openedAt.Add(age)) {
			fold = groups[:len(groups)-1]
			open = &ingest.OpenBlock{OpenedAt: openedAt}
			for _, b := range last {
				open.Events = append(open.Events, b.events...)
				open.Batches = append(open.Batches, batchInfo(b.row))
			}
		}
	}
	for _, g := range fold {
		m.BlockClosed(closedBlock(g))
	}
	// Sync also applies the rotation rule to an active segment that an
	// earlier session left at the threshold.
	if err := m.Sync(ctx); err != nil {
		return nil, err
	}

	openEvents := 0
	if open != nil {
		openEvents = len(open.Events)
	}
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.Int("hot_batches", len(rows)),
		attribute.Int("folded_blocks", len(fold)),
		attribute.Int("open_events", openEvents))
	m.cfg.Metrics.observeRebuild(time.Since(start))
	m.cfg.Logger.InfoContext(ctx, "rebuilt hot state",
		"hot_batches", len(rows), "folded_blocks", len(fold), "open_events", openEvents,
		"duration", time.Since(start))
	return open, nil
}

// loadHot checks the cheap catalog invariants and reads every hot batch with
// its inline frame, from one snapshot.
func (m *Maintainer) loadHot(ctx context.Context, cfg RebuildConfig) ([]catalog.HotBatchRow, error) {
	rtx, err := m.cfg.Session.DB().BeginRead(ctx)
	if err != nil {
		return nil, fmt.Errorf("maintainer: rebuild: %w", err)
	}
	defer func() { _ = rtx.Close(ctx) }()
	snap, err := catalog.LoadSnapshot(ctx, rtx)
	if err != nil {
		return nil, fmt.Errorf("maintainer: rebuild: load snapshot: %w", err)
	}
	if err := catalog.CheckInvariants(snap, catalog.InvariantOptions{
		MaxEventsPerBlock: m.maxEvents,
		RelayCursor:       cfg.RelayCursor,
		Cheap:             true,
	}); err != nil {
		return nil, err
	}
	rows, err := rtx.HotBatches(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("maintainer: rebuild: read hot batches: %w", err)
	}
	return rows, nil
}

// decodeHot decodes every row, fetching pointer batches ReadConcurrency at a
// time. The reads verify each object against its catalog hash.
func (m *Maintainer) decodeHot(ctx context.Context, rows []catalog.HotBatchRow) ([]hotBatch, error) {
	out := make([]hotBatch, len(rows))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(m.cfg.ReadConcurrency)
	for i, row := range rows {
		g.Go(func() error {
			frame := row.Frame
			if !row.Inline {
				var err error
				if frame, err = m.cfg.Objects.Get(gctx, row.ObjectID); err != nil {
					return fmt.Errorf("maintainer: rebuild: read hot batch [%d,%d] (object %d): %w",
						row.FirstSeq, row.LastSeq, row.ObjectID, err)
				}
			}
			evs, err := catalog.DecodeHotBatch(row, frame)
			if err != nil {
				return err
			}
			out[i] = hotBatch{row: row, events: evs}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

// groupBatches groups batches greedily, in order, into groups of at most
// maxEvents events without splitting a batch (design §10.9 step 4).
func groupBatches(batches []hotBatch, maxEvents int) ([][]hotBatch, error) {
	var groups [][]hotBatch
	var cur []hotBatch
	n := 0
	for _, b := range batches {
		if len(b.events) > maxEvents {
			// The invariant check rules this out; a block cannot hold it.
			return nil, catalog.Corruptf(catalog.SourceHotBatch, "hot batch [%d,%d] has %d events, more than a block (%d)",
				b.row.FirstSeq, b.row.LastSeq, len(b.events), maxEvents)
		}
		if n+len(b.events) > maxEvents {
			groups = append(groups, cur)
			cur, n = nil, 0
		}
		cur = append(cur, b)
		n += len(b.events)
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups, nil
}

func eventCount(g []hotBatch) int {
	n := 0
	for _, b := range g {
		n += len(b.events)
	}
	return n
}

func batchInfo(row catalog.HotBatchRow) ingest.HotBatchInfo {
	return ingest.HotBatchInfo{FirstSeq: row.FirstSeq, LastSeq: row.LastSeq, ObjectID: row.ObjectID}
}

func closedBlock(g []hotBatch) ingest.ClosedBlock {
	b := ingest.ClosedBlock{
		FirstSeq: g[0].row.FirstSeq,
		LastSeq:  g[len(g)-1].row.LastSeq,
		Events:   make([]segment.Event, 0, eventCount(g)),
		OpenedAt: g[0].row.CommittedAt,
	}
	for _, hb := range g {
		b.Events = append(b.Events, hb.events...)
		b.Batches = append(b.Batches, batchInfo(hb.row))
	}
	return b
}

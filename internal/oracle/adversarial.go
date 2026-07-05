package oracle

// Ledger-aware reconciliation for adversarial ingest traffic (#204).
// Intentional gate drops are the oracle's first PERMANENT
// expected-vs-actual divergence: the world emits input jetstream is
// contractually required to refuse. The world's AdversarialLedger
// records each lie at generation time; this file filters the expected
// side through it.
//
// The filter is one-directional-safe by construction: it only ever
// REMOVES rows from the want side. If the gate wrongly archives a lie,
// the extra row still fails CompareEventLogMultiset (and final-state
// Compare flags it as an extra record); if the gate wrongly drops an
// honest row, nothing here hides the miss. Anti-vacuity comes from the
// metric assertion (assertAdversarialDropCounters in the harness), not
// from the filter.

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// adversarialFilter answers "is this expected row / seq / record one
// the gate must drop?" from a ledger snapshot.
type adversarialFilter struct {
	// wholeEventSeqs are firehose seqs whose ENTIRE event drops
	// (invalid #sync envelope rev, verifier-rejected commits).
	wholeEventSeqs map[int64]struct{}
	// droppedOps are per-op lies keyed by (did, collection, rkey);
	// sibling rows of the same seq survive.
	droppedOps map[recordCoord]struct{}
}

type recordCoord struct {
	did, collection, rkey string
}

func newAdversarialFilter(entries []world.AdversarialEntry) *adversarialFilter {
	f := &adversarialFilter{
		wholeEventSeqs: make(map[int64]struct{}),
		droppedOps:     make(map[recordCoord]struct{}),
	}
	for _, e := range entries {
		if e.WholeEvent {
			if e.Seq > 0 {
				f.wholeEventSeqs[e.Seq] = struct{}{}
			}
			continue
		}
		f.droppedOps[recordCoord{e.DID, e.Collection, e.Rkey}] = struct{}{}
	}
	return f
}

// FilterExpectedRows removes ledger-matched rows from an expected
// event log: every row of a whole-event seq, and per-op rows whose
// (did, collection, rkey) matches a recorded lie.
func (f *adversarialFilter) FilterExpectedRows(rows []EventLogRow) []EventLogRow {
	if len(f.wholeEventSeqs) == 0 && len(f.droppedOps) == 0 {
		return rows
	}
	out := make([]EventLogRow, 0, len(rows))
	for _, row := range rows {
		if _, whole := f.wholeEventSeqs[int64(row.Seq)]; whole {
			continue
		}
		if _, dropped := f.droppedOps[recordCoord{row.DID, row.Collection, row.Rkey}]; dropped {
			continue
		}
		out = append(out, row)
	}
	return out
}

// FilterGroundTruth removes ledger-matched records from a final-state
// model. The world's own MST contains each lie (the simulator commits
// them for real so the CAR/signature pipeline stays honest); jetstream
// is required never to archive them.
func (f *adversarialFilter) FilterGroundTruth(m *Model) {
	if len(f.droppedOps) == 0 {
		return
	}
	for did, snap := range m.Accounts {
		for key := range snap.Records {
			if _, dropped := f.droppedOps[recordCoord{key.DID, key.Collection, key.Rkey}]; dropped {
				delete(snap.Records, key)
			}
		}
		m.Accounts[did] = snap
	}
}

// IsWholeEventSeq reports whether the ledger drops every row of seq —
// used by cursor-gap accounting: the consumer counts and advances past
// such an event, but no archived row ever carries its cursor.
func (f *adversarialFilter) IsWholeEventSeq(seq int64) bool {
	_, ok := f.wholeEventSeqs[seq]
	return ok
}

// ScrapeDropCounters fetches /metrics via client (the oracle wires an
// in-process pipe listener client) and returns the
// jetstream_ingest_dropped_events_total series as {source}{reason} →
// value. Series with zero values are included — the family pre-binds
// every (source, reason) pair at construction.
func ScrapeDropCounters(ctx context.Context, client *http.Client, baseURL string) (map[string]map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("oracle: build metrics request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oracle: scrape metrics: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oracle: scrape metrics: status %d", resp.StatusCode)
	}

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("oracle: parse metrics: %w", err)
	}

	out := make(map[string]map[string]float64)
	family, ok := families["jetstream_ingest_dropped_events_total"]
	if !ok {
		return nil, fmt.Errorf("oracle: metric family jetstream_ingest_dropped_events_total not exposed")
	}
	for _, m := range family.GetMetric() {
		var source, reason string
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "source":
				source = lp.GetValue()
			case "reason":
				reason = lp.GetValue()
			}
		}
		if source == "" || reason == "" {
			return nil, fmt.Errorf("oracle: dropped-events series missing source/reason labels")
		}
		if out[source] == nil {
			out[source] = make(map[string]float64)
		}
		out[source][reason] = m.GetCounter().GetValue()
	}
	return out, nil
}

// ExpectedDropFloors folds a ledger snapshot into the MINIMUM count
// each gate-owned (source, reason) series must show. Verifier-layer
// entries contribute nothing (they never reach the gate counter).
// Floors, not exact counts: independent drops (e.g. swarm-fault
// missing_block) share the family.
//
// A whole-event drop increments the counter ONCE regardless of how
// many rows it kills, and the world may ledger both the whole-event
// seq AND the per-record coordinates it permanently loses (so ground
// truth can exclude them). Count each such seq once: per-op entries
// sharing a seq with a whole-event entry are exclusion bookkeeping,
// not extra increments.
func ExpectedDropFloors(entries []world.AdversarialEntry) map[string]map[string]int {
	wholeSeqs := make(map[int64]struct{})
	for _, e := range entries {
		if e.WholeEvent && e.Seq > 0 {
			wholeSeqs[e.Seq] = struct{}{}
		}
	}
	out := make(map[string]map[string]int)
	for _, e := range entries {
		if e.Layer != world.AdversarialLayerGate {
			continue
		}
		if !e.WholeEvent && e.Seq > 0 {
			if _, shadowed := wholeSeqs[e.Seq]; shadowed {
				continue
			}
		}
		src := string(e.Source)
		if out[src] == nil {
			out[src] = make(map[string]int)
		}
		out[src][e.Reason]++
	}
	return out
}

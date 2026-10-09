package migrate

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

// inventoryListMax bounds the vacancies and unclassified keys an Inventory
// lists; the counts cover every one.
const inventoryListMax = 100

// Inventory is the dry run's report (migration plan §13 S0): what a
// migration of this archive would copy, read from the source alone.
type Inventory struct {
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at,omitzero"`
	// Error is why the inventory stopped short, if it did.
	Error string `json:"error,omitempty"`

	// Phase is the source's lifecycle phase; only steady_state migrates.
	Phase    string            `json:"phase"`
	Segments InventorySegments `json:"segments"`
	// SeqNext is the source's durable seq/next.
	SeqNext uint64 `json:"seq_next"`
	// Vacancies lists the first registered vacancies and where each sits.
	Vacancies      []InventoryVacancy `json:"vacancies,omitempty"`
	VacancyCount   int                `json:"vacancy_count"`
	VacancySeqs    uint64             `json:"vacancy_seqs"`
	MetaByPrefix   map[string]KeyStat `json:"meta_by_prefix"`
	MetaByClass    map[string]KeyStat `json:"meta_by_class"`
	Unclassified   []string           `json:"unclassified,omitempty"`
	UnclassifiedN  int64              `json:"unclassified_count"`
	SeedBytes      int64              `json:"seed_bytes"`
	SeedObjects    int64              `json:"seed_objects"`
	ReadyToMigrate bool               `json:"ready_to_migrate"`
}

// InventorySegments describes the source's main segments.
type InventorySegments struct {
	Sealed       int   `json:"sealed"`
	Active       int   `json:"active"`
	Blocks       int64 `json:"blocks"`
	Contiguous   bool  `json:"contiguous"`
	SealedBytes  int64 `json:"sealed_bytes"`
	ActiveBlocks int   `json:"active_blocks"`
}

// InventoryVacancy is one registered vacancy, [Start, End).
type InventoryVacancy struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	// Where is "inside segment N", "between segments N and M", "before
	// segment N", or "at the tip": a tip vacancy refuses a handoff until
	// an event is written after it.
	Where string `json:"where"`
}

// KeyStat counts metadata keys and their key and value bytes.
type KeyStat struct {
	Keys  int64 `json:"keys"`
	Bytes int64 `json:"bytes"`
}

// InventoryConfig configures TakeInventory.
type InventoryConfig struct {
	Meta    *pebblestore.Store
	Catalog LocalCatalog
	// FS stats the source's segment files. Nil is the OS filesystem.
	FS vfs.FS
	// ReadBytesPerSec throttles the metadata scan. Zero is unthrottled.
	ReadBytesPerSec int64
	// Progress, when set, receives the report as it fills in. Each call
	// gets its own copy, which it may keep and read from any goroutine.
	Progress func(Inventory)
}

// TakeInventory reads the source and reports what a migration would copy.
// It writes nothing. The metadata scan reads one Pebble snapshot in full,
// throttled, so on a large archive it takes a while.
func TakeInventory(ctx context.Context, cfg InventoryConfig) (Inventory, error) {
	inv := Inventory{
		StartedAt:    time.Now(),
		MetaByPrefix: map[string]KeyStat{},
		MetaByClass:  map[string]KeyStat{},
	}
	progress := func() {
		if cfg.Progress != nil {
			cfg.Progress(inv.clone())
		}
	}
	fail := func(err error) (Inventory, error) {
		inv.Error = err.Error()
		inv.FinishedAt = time.Now()
		progress()
		return inv, err
	}
	fsys := cfg.FS
	if fsys == nil {
		fsys = vfs.Default
	}

	segs := cfg.Catalog.Snapshot().Segments(catalog.Main)
	if err := inventorySegments(&inv, segs, func(idx uint64) (int64, error) {
		st, err := fsys.Stat(cfg.Catalog.Path(catalog.Main, idx))
		if err != nil {
			return 0, fmt.Errorf("migrate: stat segment %d: %w", idx, err)
		}
		return st.Size(), nil
	}); err != nil {
		return fail(err)
	}
	next, err := localNext(ctx, cfg.Meta)
	if err != nil {
		return fail(err)
	}
	inv.SeqNext = next
	phase, err := cfg.Meta.Get(ctx, []byte(lifecycle.PhaseKey))
	if err != nil && !errors.Is(err, metastore.ErrNotFound) {
		return fail(fmt.Errorf("migrate: read local %s: %w", lifecycle.PhaseKey, err))
	}
	inv.Phase = string(phase)
	gaps, err := ingest.LoadSeqGaps(cfg.Meta)
	if err != nil {
		return fail(fmt.Errorf("migrate: %w", err))
	}
	ranges := gaps.Ranges()
	inv.VacancyCount, inv.VacancySeqs = len(ranges), gaps.Width()
	for _, g := range ranges[:min(len(ranges), inventoryListMax)] {
		inv.Vacancies = append(inv.Vacancies, InventoryVacancy{Start: g.Start, End: g.End, Where: vacancyWhere(segs, g.Start)})
	}
	progress()

	sn := cfg.Meta.NewSnapshot()
	defer func() { _ = sn.Close() }()
	it, err := sn.NewIter(ctx, nil, nil)
	if err != nil {
		return fail(fmt.Errorf("migrate: iterate local metadata: %w", err))
	}
	defer func() { _ = it.Close() }()
	thr := newThrottle(cfg.ReadBytesPerSec)
	pending := 0
	for it.Next() {
		k := it.Key()
		n := int64(len(k) + len(it.Value()))
		c := classify(k)
		add(inv.MetaByPrefix, prefixOf(k), n)
		add(inv.MetaByClass, c.String(), n)
		if c == classUnknown {
			inv.UnclassifiedN++
			if len(inv.Unclassified) < inventoryListMax {
				inv.Unclassified = append(inv.Unclassified, strconv.Quote(string(k)))
			}
		}
		if pending += int(n); pending >= 1<<20 {
			if err := thr.wait(ctx, pending); err != nil {
				return fail(err)
			}
			pending = 0
			progress()
		}
	}
	if err := it.Err(); err != nil {
		return fail(fmt.Errorf("migrate: iterate local metadata: %w", err))
	}
	inv.ReadyToMigrate = inv.UnclassifiedN == 0 && inv.Segments.Contiguous && lifecycle.Phase(inv.Phase) == lifecycle.PhaseSteadyState
	inv.FinishedAt = time.Now()
	progress()
	return inv, nil
}

// clone copies inv deeply: the scan keeps writing the original's maps.
func (inv Inventory) clone() Inventory {
	inv.MetaByPrefix = maps.Clone(inv.MetaByPrefix)
	inv.MetaByClass = maps.Clone(inv.MetaByClass)
	inv.Vacancies = slices.Clone(inv.Vacancies)
	inv.Unclassified = slices.Clone(inv.Unclassified)
	return inv
}

func add(m map[string]KeyStat, k string, n int64) {
	s := m[k]
	s.Keys++
	s.Bytes += n
	m[k] = s
}

// inventorySegments fills inv.Segments and the seed estimate: each sealed
// file less its blocks' length prefixes, which uploads strip, one object
// per block and footer, and the active segment's durable blocks. Identical
// frames dedupe on upload, so both are upper bounds. SealedBytes is the
// files whole, which is what the seed reads.
func inventorySegments(inv *Inventory, segs []catalog.SegmentView, size func(uint64) (int64, error)) error {
	s := &inv.Segments
	s.Contiguous = true
	for i, v := range segs {
		if v.Index != uint64(i) || (v.State == catalog.Active && i != len(segs)-1) {
			s.Contiguous = false
		}
		s.Blocks += int64(len(v.Blocks))
		switch v.State {
		case catalog.Sealed:
			s.Sealed++
			n, err := size(v.Index)
			if err != nil {
				return err
			}
			uploaded := n - 8*int64(len(v.Blocks))
			if uploaded < 0 {
				return fmt.Errorf("migrate: segment %d holds %d bytes, fewer than its %d blocks' length prefixes", v.Index, n, len(v.Blocks))
			}
			s.SealedBytes += n
			inv.SeedBytes += uploaded
			inv.SeedObjects += int64(len(v.Blocks)) + 1
		case catalog.Active:
			s.Active++
			s.ActiveBlocks = len(v.Blocks)
			for _, b := range v.Blocks {
				inv.SeedBytes += int64(b.CompressedSize)
			}
			inv.SeedObjects += int64(len(v.Blocks))
		}
	}
	return nil
}

// vacancyWhere places a vacancy starting at seq among segs.
func vacancyWhere(segs []catalog.SegmentView, seq uint64) string {
	before := -1
	for i, v := range segs {
		lo, hi, ok := seqEnvelope(v.Blocks)
		if !ok {
			continue
		}
		if seq >= lo && seq <= hi {
			return fmt.Sprintf("inside segment %d", v.Index)
		}
		if seq > hi {
			before = i
			continue
		}
		if before < 0 {
			return fmt.Sprintf("before segment %d", v.Index)
		}
		return fmt.Sprintf("between segments %d and %d", segs[before].Index, v.Index)
	}
	return "at the tip"
}

// seqEnvelope is the seq range of blocks' non-empty blocks.
func seqEnvelope(blocks []segment.BlockInfo) (lo, hi uint64, ok bool) {
	for _, b := range blocks {
		if b.EventCount == 0 {
			continue
		}
		if !ok || b.MinSeq < lo {
			lo = b.MinSeq
		}
		hi, ok = max(hi, b.MaxSeq), true
	}
	return lo, hi, ok
}

// InventoryMetrics is the dry run's Prometheus state:
// jetstream_migration_inventory_*. All methods are nil-safe.
type InventoryMetrics struct {
	metaKeys     *prometheus.GaugeVec
	metaBytes    *prometheus.GaugeVec
	unclassified prometheus.Gauge
	seedBytes    prometheus.Gauge
	seedObjects  prometheus.Gauge
	vacancies    prometheus.Gauge
	finished     prometheus.Gauge
}

// NewInventoryMetrics registers the dry run's series against reg.
func NewInventoryMetrics(reg prometheus.Registerer) *InventoryMetrics {
	o := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: "jetstream", Subsystem: "migration_inventory", Name: name, Help: help}
	}
	m := &InventoryMetrics{
		metaKeys:     prometheus.NewGaugeVec(prometheus.GaugeOpts(o("meta_keys", "Source metadata keys by migration rule.")), []string{"class"}),
		metaBytes:    prometheus.NewGaugeVec(prometheus.GaugeOpts(o("meta_bytes", "Source metadata key and value bytes by migration rule.")), []string{"class"}),
		unclassified: prometheus.NewGauge(prometheus.GaugeOpts(o("unclassified_keys", "Source metadata keys with no migration rule; a migration refuses to start past one."))),
		seedBytes:    prometheus.NewGauge(prometheus.GaugeOpts(o("seed_bytes", "Segment bytes a migration would upload, at most."))),
		seedObjects:  prometheus.NewGauge(prometheus.GaugeOpts(o("seed_objects", "Objects a migration would upload, at most."))),
		vacancies:    prometheus.NewGauge(prometheus.GaugeOpts(o("vacancies", "Registered seq vacancies a migration would carry over."))),
		finished:     prometheus.NewGauge(prometheus.GaugeOpts(o("finished_timestamp_seconds", "When the inventory finished; 0 until it does."))),
	}
	if reg != nil {
		reg.MustRegister(m.metaKeys, m.metaBytes, m.unclassified, m.seedBytes, m.seedObjects, m.vacancies, m.finished)
	}
	return m
}

// Set publishes inv.
func (m *InventoryMetrics) Set(inv Inventory) {
	if m == nil {
		return
	}
	classes := make([]string, 0, len(inv.MetaByClass))
	for c := range inv.MetaByClass {
		classes = append(classes, c)
	}
	slices.Sort(classes)
	for _, c := range classes {
		m.metaKeys.WithLabelValues(c).Set(float64(inv.MetaByClass[c].Keys))
		m.metaBytes.WithLabelValues(c).Set(float64(inv.MetaByClass[c].Bytes))
	}
	m.unclassified.Set(float64(inv.UnclassifiedN))
	m.seedBytes.Set(float64(inv.SeedBytes))
	m.seedObjects.Set(float64(inv.SeedObjects))
	m.vacancies.Set(float64(inv.VacancyCount))
	if !inv.FinishedAt.IsZero() && inv.Error == "" {
		m.finished.Set(float64(inv.FinishedAt.Unix()))
	}
}

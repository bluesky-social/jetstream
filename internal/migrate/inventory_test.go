package migrate

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore/pebblestore"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// pathCatalog is a viewCatalog whose segments have files.
type pathCatalog struct{ viewCatalog }

func (p *pathCatalog) Snapshot() catalog.CatalogView { return p }
func (p *pathCatalog) Path(_ catalog.Namespace, idx uint64) string {
	return fmt.Sprintf("/segs/%d", idx)
}

func TestTakeInventory(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fs := vfs.NewMem()
	st, err := pebblestore.Open("/data", nil, pebblestore.WithFS(fs))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	gap := func(start, end uint64) (k, v []byte) {
		k = binary.BigEndian.AppendUint64([]byte("seq/gap/"), start)
		v = binary.BigEndian.AppendUint64([]byte{1, 1}, end)
		return k, v
	}
	b := st.NewBatch()
	for k, v := range map[string]string{
		"repo/did:plc:a":       "x",
		"repo/did:plc:b":       "yy",
		lifecycle.PhaseKey:     string(lifecycle.PhaseSteadyState),
		catalog.RelayCursorKey: "c",
		"sync/identity/did:a":  "drop",
		"mystery/key":          "?",
	} {
		b.Set([]byte(k), []byte(v))
	}
	b.Set([]byte(catalog.MainSeqKey), catalog.EncodeSeq(80))
	// Vacancies inside segment 0, between segments 0 and 1, and at the tip.
	for _, g := range [][2]uint64{{5, 6}, {11, 20}, {70, 80}} {
		k, v := gap(g[0], g[1])
		b.Set(k, v)
	}
	require.NoError(t, b.Commit(ctx))

	blocks := func(ranges ...[2]uint64) []segment.BlockInfo {
		var out []segment.BlockInfo
		for _, r := range ranges {
			out = append(out, segment.BlockInfo{MinSeq: r[0], MaxSeq: r[1], EventCount: uint32(r[1] - r[0] + 1), CompressedSize: 100})
		}
		return out
	}
	cat := &pathCatalog{viewCatalog{segs: []catalog.SegmentView{
		{Namespace: catalog.Main, Index: 0, State: catalog.Sealed, Blocks: blocks([2]uint64{1, 4}, [2]uint64{7, 10})},
		{Namespace: catalog.Main, Index: 1, State: catalog.Sealed, Blocks: blocks([2]uint64{20, 40})},
		{Namespace: catalog.Main, Index: 2, State: catalog.Active, Blocks: blocks([2]uint64{41, 69})},
	}}}
	require.NoError(t, fs.MkdirAll("/segs", 0o755))
	for idx, size := range []int{1000, 2000} {
		f, err := fs.Create(cat.Path(catalog.Main, uint64(idx)))
		require.NoError(t, err)
		_, err = f.Write(make([]byte, size))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	var updates int
	inv, err := TakeInventory(ctx, InventoryConfig{Meta: st, Catalog: cat, FS: fs, Progress: func(Inventory) { updates++ }})
	require.NoError(t, err)
	require.NotZero(t, updates)
	require.False(t, inv.FinishedAt.IsZero())

	require.Equal(t, InventorySegments{Sealed: 2, Active: 1, Blocks: 4, Contiguous: true, SealedBytes: 3000, ActiveBlocks: 1}, inv.Segments)
	require.Equal(t, int64(3000-3*8+100), inv.SeedBytes, "uploads strip each sealed block's length prefix")
	require.Equal(t, int64(2+1+1+1+1), inv.SeedObjects, "a block and a footer per sealed block and segment, plus the active block")

	require.Equal(t, string(lifecycle.PhaseSteadyState), inv.Phase)
	require.Equal(t, uint64(80), inv.SeqNext)
	require.Equal(t, 3, inv.VacancyCount)
	require.Equal(t, uint64(1+9+10), inv.VacancySeqs)
	require.Equal(t, []InventoryVacancy{
		{Start: 5, End: 6, Where: "inside segment 0"},
		{Start: 11, End: 20, Where: "between segments 0 and 1"},
		{Start: 70, End: 80, Where: "at the tip"},
	}, inv.Vacancies)

	require.Equal(t, KeyStat{Keys: 2, Bytes: int64(len("repo/did:plc:a") + 1 + len("repo/did:plc:b") + 2)}, inv.MetaByPrefix["repo/"])
	require.Equal(t, int64(2), inv.MetaByClass["copy"].Keys)
	require.Equal(t, int64(1), inv.MetaByClass["phase"].Keys)
	require.Equal(t, int64(1), inv.MetaByClass["handoff"].Keys)
	require.Equal(t, int64(1+1+3), inv.MetaByClass["drop"].Keys, "the identity cache, the seq key, and the vacancies")
	require.Equal(t, int64(1), inv.UnclassifiedN)
	require.Equal(t, []string{`"mystery/key"`}, inv.Unclassified)
	require.False(t, inv.ReadyToMigrate, "a key with no migration rule")

	// Without it, the archive is ready.
	require.NoError(t, st.Delete(ctx, []byte("mystery/key")))
	inv, err = TakeInventory(ctx, InventoryConfig{Meta: st, Catalog: cat, FS: fs})
	require.NoError(t, err)
	require.True(t, inv.ReadyToMigrate)

	// Nor does an archive that is not in steady state.
	require.NoError(t, st.Set(ctx, []byte(lifecycle.PhaseKey), []byte(lifecycle.PhaseBootstrap)))
	inv, err = TakeInventory(ctx, InventoryConfig{Meta: st, Catalog: cat, FS: fs})
	require.NoError(t, err)
	require.False(t, inv.ReadyToMigrate)
	require.NoError(t, st.Set(ctx, []byte(lifecycle.PhaseKey), []byte(lifecycle.PhaseSteadyState)))

	// A missing segment file stops the inventory with the reason.
	cat.segs = append(cat.segs[:2:2], catalog.SegmentView{Namespace: catalog.Main, Index: 2, State: catalog.Sealed}, cat.segs[2])
	cat.segs[3].Index = 3
	inv, err = TakeInventory(ctx, InventoryConfig{Meta: st, Catalog: cat, FS: fs})
	require.Error(t, err)
	require.Contains(t, inv.Error, "segment 2")
}

func TestVacancyWhere(t *testing.T) {
	t.Parallel()
	seg := func(idx uint64, lo, hi uint64) catalog.SegmentView {
		return catalog.SegmentView{Index: idx, Blocks: []segment.BlockInfo{{MinSeq: lo, MaxSeq: hi, EventCount: 1}}}
	}
	// An empty segment (compacted to nothing) has no envelope and is
	// passed over.
	segs := []catalog.SegmentView{seg(0, 10, 20), {Index: 1, Blocks: []segment.BlockInfo{{}}}, seg(2, 30, 40)}
	require.Equal(t, "before segment 0", vacancyWhere(segs, 1))
	require.Equal(t, "inside segment 0", vacancyWhere(segs, 15))
	require.Equal(t, "between segments 0 and 2", vacancyWhere(segs, 25))
	require.Equal(t, "at the tip", vacancyWhere(segs, 41))
	require.Equal(t, "at the tip", vacancyWhere(nil, 1))
}

// TestTakeInventoryProgressCopies reads every report Progress hands out,
// on another goroutine, while the scan carries on; run under -race.
func TestTakeInventoryProgressCopies(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, err := pebblestore.Open("/data", nil, pebblestore.WithFS(vfs.NewMem()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	b := st.NewBatch()
	val := make([]byte, 4<<10)
	for i := range 1500 {
		b.Set(fmt.Appendf(nil, "p%03d/%d", i%300, i), val)
	}
	require.NoError(t, b.Commit(ctx))

	reports := make(chan Inventory, 64)
	read := make(chan int)
	go func() {
		n := 0
		for inv := range reports {
			_, err := json.Marshal(inv)
			if err == nil {
				n++
			}
		}
		read <- n
	}()
	_, err = TakeInventory(ctx, InventoryConfig{Meta: st, Catalog: &viewCatalog{}, Progress: func(inv Inventory) { reports <- inv }})
	close(reports)
	require.NoError(t, err)
	require.Greater(t, <-read, 3, "the scan reported progress several times")
}

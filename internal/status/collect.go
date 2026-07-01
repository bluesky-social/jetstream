package status

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/live"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/bluesky-social/jetstream/internal/version"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/identity"
)

// keyspacePrefixes lists the pebble prefixes the status page exposes
// in PebbleStats.KeyspaceCounts. sync/identity/ is intentionally
// excluded from the public surface.
var keyspacePrefixes = []string{
	"repo/",
	"sync/chain/",
	"sync/host/",
	"relay/",
}

const topFailingHostLimit = 10

const (
	hostSortLargest = "largest"
	hostSortFailing = "failing"
)

func collectProcess(now time.Time, startedAt time.Time) ProcessInfo {
	info := version.Get()
	return ProcessInfo{
		Version:   info.Version,
		Commit:    info.Commit,
		BuiltAt:   info.Date,
		StartedAt: startedAt,
		Uptime:    now.Sub(startedAt),
		GoVersion: runtime.Version(),
	}
}

func collectPhase(s *store.Store) (PhaseInfo, error) {
	p, err := lifecycle.ReadPhase(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	at, err := lifecycle.ReadPhaseEnteredAt(s)
	if err != nil {
		return PhaseInfo{}, err
	}
	return PhaseInfo{Phase: p, PhaseEnteredAt: at}, nil
}

func collectLive(s *store.Store) (LiveStats, error) {
	cur, err := live.LoadUpstreamCursor(s, live.CursorKey)
	if err != nil {
		return LiveStats{}, err
	}
	nextSeq, _, err := s.GetUint64LE(live.SteadySeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	bootSeq, _, err := s.GetUint64LE(live.BootstrapSeqKey)
	if err != nil {
		return LiveStats{}, err
	}
	return LiveStats{
		UpstreamCursor: cur,
		NextSeq:        nextSeq,
		BootstrapSeq:   bootSeq,
	}, nil
}

func collectBackfill(s *store.Store) (BackfillStats, error) {
	counts, err := backfill.CountStatuses(s)
	if err != nil {
		return BackfillStats{}, err
	}
	cursor, err := backfill.LoadListReposCursor(s)
	if err != nil {
		return BackfillStats{}, err
	}
	timing, err := lifecycle.ReadBackfillTiming(s)
	if err != nil {
		return BackfillStats{}, err
	}
	pct := 0.0
	if counts.Total > 0 {
		pct = float64(counts.Complete) / float64(counts.Total) * 100
	}
	return BackfillStats{
		TotalDIDs:       counts.Total,
		Discovered:      counts.Discovered,
		Pending:         counts.Pending,
		Complete:        counts.Complete,
		Failed:          counts.Failed,
		Unavailable:     counts.Unavailable,
		PercentComplete: pct,
		ListReposCursor: cursor,
		StartedAt:       timing.StartedAt,
		CompletedAt:     timing.CompletedAt,
		Duration:        timing.Duration(),
	}, nil
}

func collectBackfillFast(s *store.Store) (BackfillStats, error) {
	cursor, err := backfill.LoadListReposCursor(s)
	if err != nil {
		return BackfillStats{}, err
	}
	timing, err := lifecycle.ReadBackfillTiming(s)
	if err != nil {
		return BackfillStats{}, err
	}
	// Exact counts require scanning every repo/<did> row. On production
	// instances /status uses the maintained aggregate so snapshot
	// builds stay cheap; missing aggregates render as zeros.
	counts, ok, err := backfill.LoadCounts(s)
	if err != nil {
		return BackfillStats{}, err
	}
	pct := 0.0
	if ok && counts.Total > 0 {
		pct = float64(counts.Complete) / float64(counts.Total) * 100
	}
	return BackfillStats{
		TotalDIDs:       counts.Total,
		Discovered:      counts.Discovered,
		Pending:         counts.Pending,
		Complete:        counts.Complete,
		Failed:          counts.Failed,
		Unavailable:     counts.Unavailable,
		PercentComplete: pct,
		ListReposCursor: cursor,
		StartedAt:       timing.StartedAt,
		CompletedAt:     timing.CompletedAt,
		Duration:        timing.Duration(),
	}, nil
}

func normalizeRequest(req Request) Request {
	req.Tab = strings.ToLower(strings.TrimSpace(req.Tab))
	switch req.Tab {
	case "", "summary":
		req.Tab = "summary"
	case "hosts", "accounts", "collections", "segments":
	default:
		req.Tab = "summary"
	}
	req.HostSort = strings.ToLower(strings.TrimSpace(req.HostSort))
	if req.Tab == "hosts" {
		switch req.HostSort {
		case "", hostSortLargest:
			req.HostSort = hostSortLargest
		case hostSortFailing:
		default:
			req.HostSort = hostSortLargest
		}
	} else {
		req.HostSort = ""
	}
	req.Account = strings.TrimSpace(req.Account)
	req.DID = strings.TrimSpace(req.DID)
	req.Handle = strings.TrimSpace(req.Handle)
	if req.Tab != "accounts" {
		req.Account = ""
		req.DID = ""
		req.Handle = ""
		return req
	}
	if req.Account == "" {
		switch {
		case req.DID != "":
			req.Account = req.DID
		case req.Handle != "":
			req.Account = req.Handle
		}
	}
	req.DID = ""
	req.Handle = ""
	return req
}

func collectHosts(s *store.Store, sortBy string) (HostDiagnostics, error) {
	statuses, err := backfill.ListHostStatuses(s)
	if err != nil {
		return HostDiagnostics{}, err
	}
	rows := make([]HostRow, 0, len(statuses))
	for i := range statuses {
		rows = append(rows, hostRowFromBackfill(&statuses[i]))
	}
	sortHostRows(rows, sortBy)
	failingRows := append([]HostRow(nil), rows...)
	sortHostRows(failingRows, hostSortFailing)
	top := make([]HostRow, 0, min(topFailingHostLimit, len(failingRows)))
	for _, row := range failingRows {
		if row.Failed == 0 {
			continue
		}
		top = append(top, row)
		if len(top) == topFailingHostLimit {
			break
		}
	}
	return HostDiagnostics{Rows: rows, TopFailing: top}, nil
}

func sortHostRows(rows []HostRow, sortBy string) {
	sort.Slice(rows, func(i, j int) bool {
		if sortBy == hostSortFailing && rows[i].Failed != rows[j].Failed {
			return rows[i].Failed > rows[j].Failed
		}
		if rows[i].Total != rows[j].Total {
			return rows[i].Total > rows[j].Total
		}
		if rows[i].Failed != rows[j].Failed {
			return rows[i].Failed > rows[j].Failed
		}
		return rows[i].Host < rows[j].Host
	})
}

func hostRowFromBackfill(hs *backfill.HostStatus) HostRow {
	row := HostRow{
		Host:             hs.Host,
		Total:            hs.Total,
		Active:           hs.Active,
		NotStarted:       hs.NotStarted,
		Pending:          hs.Pending,
		Complete:         hs.Complete,
		Failed:           hs.Failed,
		Unavailable:      hs.Unavailable,
		LastAttemptedAt:  hs.LastAttemptedAt,
		LatestError:      hs.LatestError,
		LatestErrorClass: string(hs.LatestErrorClass),
	}
	if len(hs.ErrorClassCounts) > 0 {
		row.ErrorClassCounts = make(map[string]uint64, len(hs.ErrorClassCounts))
		for class, count := range hs.ErrorClassCounts {
			row.ErrorClassCounts[string(class)] = count
		}
	}
	if len(hs.RecentErrors) > 0 {
		row.RecentErrors = make([]HostErrorRow, 0, len(hs.RecentErrors))
		for _, sample := range hs.RecentErrors {
			row.RecentErrors = append(row.RecentErrors, HostErrorRow{
				DID:         string(sample.DID),
				AttemptedAt: sample.AttemptedAt,
				Class:       string(sample.Class),
				Error:       sample.Error,
			})
		}
	}
	return row
}

func collectAccount(ctx context.Context, s *store.Store, resolver identity.Resolver, req Request) AccountLookup {
	acct := AccountLookup{}
	var did atmos.DID
	var ok bool
	var err error

	account := strings.TrimSpace(req.Account)
	if account == "" {
		return acct
	}

	if strings.HasPrefix(strings.ToLower(account), "did:") {
		acct.QueryKind = "did"
		acct.Query = account
		did, err = atmos.ParseDID(account)
		if err != nil {
			acct.Error = fmt.Sprintf("invalid DID: %v", err)
			return acct
		}
	} else {
		handle := strings.TrimPrefix(account, "@")
		acct.QueryKind = "handle"
		acct.Query = handle
		parsed, err := atmos.ParseHandle(handle)
		if err != nil {
			acct.Error = fmt.Sprintf("invalid handle: %v", err)
			return acct
		}
		handle = string(parsed.Normalize())
		acct.Query = handle
		if resolver != nil {
			did, err = resolver.ResolveHandle(ctx, parsed.Normalize())
			if err != nil {
				acct.Error = fmt.Sprintf("resolve handle %q: %v", handle, err)
				return acct
			}
		} else {
			did, ok, err = backfill.LookupDIDByHandle(s, handle)
			if err != nil {
				acct.Error = err.Error()
				return acct
			}
			if !ok {
				return acct
			}
		}
	}

	rs, found, err := backfill.LoadRepoStatus(s, did)
	if err != nil {
		acct.Error = err.Error()
		return acct
	}
	if !found {
		acct.DID = string(did)
		return acct
	}

	acct.Found = true
	acct.DID = string(did)
	acct.Handle = rs.Handle
	if acct.Handle == "" {
		acct.Handle = fallbackDeclaredHandle(ctx, resolver, acct.QueryKind, acct.Query, did)
	}
	acct.PDS = rs.PDS
	acct.Host = rs.Host
	acct.Active = rs.Active
	acct.Backfill = string(rs.Backfill.Status)
	acct.Attempts = rs.Backfill.Attempts
	acct.LastError = rs.Backfill.LastError
	acct.BackfillRev = rs.Backfill.Rev
	acct.LatestRev = rs.Rev
	acct.UpdatedAt = rs.UpdatedAt
	acct.LastAttemptedAt = rs.LastAttemptedAt
	acct.RecordCount = rs.RecordCount
	acct.TotalBytes = rs.TotalBytes

	return acct
}

func fallbackDeclaredHandle(ctx context.Context, resolver identity.Resolver, queryKind, query string, did atmos.DID) string {
	if queryKind == "handle" && query != "" {
		return query
	}
	if resolver == nil {
		return ""
	}
	doc, err := resolver.ResolveDID(ctx, did)
	if err != nil || doc == nil {
		return ""
	}
	ident, err := identity.IdentityFromDocument(doc)
	if err != nil || ident.Handle == "" || ident.Handle == atmos.HandleInvalid {
		return ""
	}
	return string(ident.Handle)
}

func treeFromManifest(ms manifest.SegmentTreeStats) TreeAggregate {
	tree := TreeAggregate{
		Dir:               ms.Dir,
		SealedCount:       ms.SealedCount,
		ActiveCount:       ms.ActiveCount,
		CompressedBytes:   ms.CompressedBytes,
		UncompressedBytes: ms.UncompressedBytes,
		DiskBytes:         ms.DiskBytes,
		EventCount:        ms.EventCount,
		BlockCount:        ms.BlockCount,
		OldestMTime:       ms.OldestMTime,
		NewestMTime:       ms.NewestMTime,
		MinSeq:            ms.MinSeq,
		MaxSeq:            ms.MaxSeq,
		MinWitnessedAt:    microsToTime(ms.MinWitnessedAt),
		MaxWitnessedAt:    microsToTime(ms.MaxWitnessedAt),
	}
	if ms.LatestSegment != nil {
		tree.LatestSegment = &SegmentSummary{
			Index:           ms.LatestSegment.Index,
			Sealed:          true,
			EventCount:      ms.LatestSegment.EventCount,
			UniqueDIDCount:  ms.LatestSegment.UniqueDIDCount,
			BlockCount:      ms.LatestSegment.BlockCount,
			CollectionCount: ms.LatestSegment.CollectionCount,
			MinSeq:          ms.LatestSegment.MinSeq,
			MaxSeq:          ms.LatestSegment.MaxSeq,
			MinWitnessedAt:  microsToTime(ms.LatestSegment.MinWitnessedAt),
			MaxWitnessedAt:  microsToTime(ms.LatestSegment.MaxWitnessedAt),
			SizeBytes:       ms.LatestSegment.SizeBytes,
		}
	}
	return tree
}

func collectionsFromManifest(ms manifest.SegmentTreeStats) map[string]*CollectionAggregate {
	collections := make([]CollectionAggregate, 0, len(ms.Collections))
	for _, c := range ms.Collections {
		collections = append(collections, CollectionAggregate{
			NSID:         c.NSID,
			EventCount:   c.EventCount,
			SegmentCount: c.SegmentCount,
			BlockCount:   c.BlockCount,
		})
	}
	out := make(map[string]*CollectionAggregate, len(collections))
	for i := range collections {
		c := collections[i]
		out[c.NSID] = &c
	}
	return out
}

func collectManifestSegmentAggregate(ms manifest.SegmentTreeStats, roots []string) (*SegmentAggregate, error) {
	tree := treeFromManifest(ms)
	collections := collectionsFromManifest(ms)

	activeTail, tailWarnings, err := scanActiveTail(roots[0], collections)
	if err != nil {
		return nil, err
	}
	mergeTree(&tree, activeTail)

	liveTree, liveWarnings, err := scanTree(roots[1], InspectAllOptions{}, collections)
	if err != nil {
		return nil, err
	}

	agg := &SegmentAggregate{
		Trees: []TreeAggregate{
			tree,
			liveTree,
		},
		Warnings: append(tailWarnings, liveWarnings...),
	}
	agg.Collections = materializeCollections(collections)
	agg.Network = computeNetworkTotals(agg.Trees, len(agg.Collections))
	return agg, nil
}

func scanActiveTail(root string, collections map[string]*CollectionAggregate) (TreeAggregate, []string, error) {
	tree := TreeAggregate{Dir: root}
	files, err := ingest.SegmentFiles(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return tree, nil, nil
		}
		return TreeAggregate{}, nil, err
	}
	if len(files) == 0 {
		return tree, nil, nil
	}

	tail := files[len(files)-1]
	info, err := os.Stat(tail.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return tree, nil, nil
		}
		return TreeAggregate{}, nil, fmt.Errorf("status: stat %s: %w", tail.Path, err)
	}
	ins, inspectErr := segment.Inspect(tail.Path)
	if inspectErr != nil {
		// Same tolerance as InspectAll: the tail can be mid-rotation.
		return tree, nil, nil //nolint:nilerr
	}
	if ins.Sealed {
		return tree, nil, nil
	}

	tree.OldestMTime = info.ModTime()
	tree.NewestMTime = info.ModTime()
	tree.ActiveCount = 1
	tree.DiskBytes = ins.FileSize
	tree.LatestSegment = &SegmentSummary{
		Index:           tail.Idx,
		Sealed:          false,
		EventCount:      ins.TotalEvents,
		UniqueDIDCount:  ins.UniqueDIDCount,
		BlockCount:      uint32(len(ins.Blocks)),
		CollectionCount: len(ins.Collections),
		MinSeq:          ins.MinSeq,
		MaxSeq:          ins.MaxSeq,
		MinWitnessedAt:  microsToTime(ins.MinWitnessedAt),
		MaxWitnessedAt:  microsToTime(ins.MaxWitnessedAt),
		SizeBytes:       ins.FileSize,
	}
	foldInspection(&tree, ins, collections)
	return tree, nil, nil
}

func mergeTree(dst *TreeAggregate, src TreeAggregate) {
	dst.SealedCount += src.SealedCount
	dst.ActiveCount += src.ActiveCount
	dst.CompressedBytes += src.CompressedBytes
	dst.UncompressedBytes += src.UncompressedBytes
	dst.DiskBytes += src.DiskBytes
	dst.EventCount += src.EventCount
	dst.BlockCount += src.BlockCount

	if !src.OldestMTime.IsZero() && (dst.OldestMTime.IsZero() || src.OldestMTime.Before(dst.OldestMTime)) {
		dst.OldestMTime = src.OldestMTime
	}
	if src.NewestMTime.After(dst.NewestMTime) {
		dst.NewestMTime = src.NewestMTime
	}
	if src.EventCount > 0 {
		if dst.MinSeq == 0 || src.MinSeq < dst.MinSeq {
			dst.MinSeq = src.MinSeq
		}
		if src.MaxSeq > dst.MaxSeq {
			dst.MaxSeq = src.MaxSeq
		}
		if !src.MinWitnessedAt.IsZero() && (dst.MinWitnessedAt.IsZero() || src.MinWitnessedAt.Before(dst.MinWitnessedAt)) {
			dst.MinWitnessedAt = src.MinWitnessedAt
		}
		if src.MaxWitnessedAt.After(dst.MaxWitnessedAt) {
			dst.MaxWitnessedAt = src.MaxWitnessedAt
		}
	}
	if src.LatestSegment != nil && (dst.LatestSegment == nil || src.LatestSegment.Index >= dst.LatestSegment.Index) {
		dst.LatestSegment = src.LatestSegment
	}
}

func collectPebble(s *store.Store, dataDir string) (PebbleStats, error) {
	stats := PebbleStats{KeyspaceCounts: make(map[string]uint64, len(keyspacePrefixes))}

	// On-disk size of meta.pebble/.
	pebbleDir := filepath.Join(dataDir, store.PebbleSubdir)
	if err := filepath.WalkDir(pebbleDir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		stats.DiskBytes += fi.Size()
		return nil
	}); err != nil {
		return PebbleStats{}, fmt.Errorf("status: walk %s: %w", pebbleDir, err)
	}

	// Per-prefix key counts.
	for _, prefix := range keyspacePrefixes {
		c, err := countKeysWithPrefix(s, prefix)
		if err != nil {
			return PebbleStats{}, err
		}
		stats.KeyspaceCounts[prefix] = c
	}
	return stats, nil
}

func collectPebbleFast() PebbleStats {
	return PebbleStats{KeyspaceCounts: make(map[string]uint64, len(keyspacePrefixes))}
}

func countKeysWithPrefix(s *store.Store, prefix string) (uint64, error) {
	lower := []byte(prefix)
	upper := store.PrefixUpperBound(lower)

	it, err := s.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("status: open iter %q: %w", prefix, err)
	}
	defer func() { _ = it.Close() }()

	var n uint64
	for it.First(); it.Valid(); it.Next() {
		n++
	}
	if err := it.Error(); err != nil {
		return 0, fmt.Errorf("status: iter %q: %w", prefix, err)
	}
	return n, nil
}

// build composes the gather functions into a Snapshot. ctx is checked
// once at entry — gather functions do not currently accept ctx, so
// per-section checks would be theater. If/when individual gather
// functions take ctx (e.g. context-aware pebble iteration), add the
// per-section checks back.
func build(ctx context.Context, opts Options, startedAt time.Time) (*Snapshot, error) {
	now := opts.Now()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	phase, err := collectPhase(opts.Store)
	if err != nil {
		return nil, err
	}

	liveStats, err := collectLive(opts.Store)
	if err != nil {
		return nil, err
	}

	var (
		bf  BackfillStats
		agg *SegmentAggregate
		pdb PebbleStats
	)

	roots := []string{
		filepath.Join(opts.DataDir, "segments"),
		filepath.Join(opts.DataDir, "backfill", "live_segments"),
	}
	if opts.Manifest != nil {
		if err := opts.Manifest.Wait(ctx); err != nil {
			return nil, err
		}
		bf, err = collectBackfillFast(opts.Store)
		if err != nil {
			return nil, err
		}
		agg, err = collectManifestSegmentAggregate(opts.Manifest.SegmentStats(), roots)
		if err != nil {
			return nil, err
		}
		pdb = collectPebbleFast()
	} else {
		bf, err = collectBackfill(opts.Store)
		if err != nil {
			return nil, err
		}
		agg, err = InspectAll(roots, InspectAllOptions{})
		if err != nil {
			return nil, err
		}
		pdb, err = collectPebble(opts.Store, opts.DataDir)
		if err != nil {
			return nil, err
		}
	}
	if len(agg.Trees) != 2 {
		return nil, fmt.Errorf("status: segment aggregate has %d trees, expected 2 (segments + backfill/live_segments); the /status template assumes this shape", len(agg.Trees))
	}

	snap := &Snapshot{
		GeneratedAt:      now,
		Process:          collectProcess(now, startedAt),
		Phase:            phase,
		Backfill:         bf,
		Live:             liveStats,
		SegmentAggregate: agg,
		Pebble:           pdb,
	}

	snap.CursorLookback.ConfiguredLookback = opts.CursorLookback
	if opts.Manifest != nil && opts.CursorLookback > 0 {
		snap.CursorLookback.ManifestSegmentCount = opts.Manifest.SegmentCount()
		seq, ts := opts.Manifest.LookbackFloor(opts.CursorLookback)
		snap.CursorLookback.OldestRetainedSeq = seq
		if ts != 0 {
			snap.CursorLookback.OldestRetainedAt = time.UnixMicro(ts)
		}
	}

	return snap, nil
}

func buildForRequest(ctx context.Context, opts Options, startedAt time.Time, req Request) (*Snapshot, error) {
	req = normalizeRequest(req)
	snap, err := build(ctx, opts, startedAt)
	if err != nil {
		return nil, err
	}
	snap.Request = req

	switch req.Tab {
	case "hosts":
		hosts, err := collectHosts(opts.Store, req.HostSort)
		if err != nil {
			return nil, err
		}
		snap.Hosts = hosts
	case "accounts":
		snap.Account = collectAccount(ctx, opts.Store, opts.IdentityResolver, req)
	}

	return snap, nil
}

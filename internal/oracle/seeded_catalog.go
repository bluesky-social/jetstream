package oracle

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/ingest/maintainer"
	"github.com/bluesky-social/jetstream/internal/ingest/syncstate"
	"github.com/bluesky-social/jetstream/internal/lifecycle"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/simulator/world"
	"github.com/bluesky-social/jetstream/segment"
)

// seedEpoch is the default fixture clock: the simulator's logical epoch, so
// seeded timestamps sort near the world's own revs.
var seedEpoch = time.UnixMicro(1_700_000_000_000_000).UTC()

// seedMetaChunk bounds the ops in one CommitMeta so a large seed never
// builds one oversized metadata transaction.
const seedMetaChunk = 256

// SeedCatalogConfig configures SeedCatalog.
type SeedCatalogConfig struct {
	// World must be bootstrapped with its runtime attached. SeedCatalog
	// archives every repo, then generates LiveEvents firehose frames and
	// archives them too, so the world's firehose tip is the seeded
	// relay/cursor on return.
	World      *world.World
	LiveEvents int
	// Session writes the catalog. The caller holds its lease. The catalog
	// must be empty: SeedCatalog initializes the main namespace.
	Session  *catalog.Session
	Uploader ingest.ObjectUploader
	// Zero selects segment.DefaultMaxEventsPerBlock and
	// maintainer.DefaultMaxSegmentBytes. A caller that later runs a
	// maintainer over the catalog should pass the same values.
	MaxEventsPerBlock int
	MaxSegmentBytes   int64
	// Start is the fixture clock. Zero selects the simulator epoch.
	Start time.Time
}

// SeededCatalog is what SeedCatalog archived: the oracle's expectations for
// a catalog that entered steady state before any process started.
type SeededCatalog struct {
	// Events is the archived main log in seq order: Events[i].Seq == i+1.
	// Live rows carry the upstream seq in UpstreamRelayCursor; backfill
	// rows carry zero, as in local mode.
	Events []segment.Event
	// BackfillEvents is the number of leading backfill rows in Events.
	BackfillEvents int
	// NextSeq is the committed seq/next.
	NextSeq uint64
	// LiveStartCursor is the world's firehose seq before the live frames
	// were generated; RelayCursor is the committed relay/cursor, the
	// world's firehose tip on return.
	LiveStartCursor int64
	RelayCursor     int64
	SealedSegments  int
	ActiveBlocks    int
	// Repos is the number of repo/ rows.
	Repos int
}

// SeedCatalog builds a catalog already in steady_state from a simulator
// world: sealed main segments, an active segment holding blocks, repo/
// rows, backfill counts and timing, verifier chain state, and relay/cursor.
//
// It writes what local-mode bootstrap followed by merge leaves behind
// without running either: each repo's backfill rows (from the same CAR
// getRepo serves), then the rows ingest derives from each live frame, in
// blocks committed through CommitBlock and rotated by the maintainer's
// rule, finishing with the metadata in CommitMeta transactions. Everything
// goes through catalog.Session and ingest.ObjectUploader, so the fixture
// runs on storagefake with memblob or on PostgreSQL with S3, and leaves
// catalog.CheckInvariants clean. Output is a function of the world's seed.
//
// Differences from a real bootstrap: backfill never overlaps live traffic;
// chain state exists only for DIDs a live commit touched; host/, handle/,
// pdshost/, the identity cache, and the listRepos cursor are absent; and a
// repo row names no PDS. Archived rows live in blocks, not hot batches, so
// a storagefake RelayWatch must only Expect upstream seqs after
// RelayCursor. The world must not carry adversarial entries or generate
// #account frames: the fixture does not model their drops or hosting state.
func SeedCatalog(ctx context.Context, cfg SeedCatalogConfig) (*SeededCatalog, error) {
	switch {
	case cfg.World == nil || cfg.Session == nil || cfg.Uploader == nil:
		return nil, errors.New("oracle: seed catalog needs a world, session, and uploader")
	case cfg.LiveEvents < 0 || cfg.MaxEventsPerBlock < 0 || cfg.MaxSegmentBytes < 0:
		return nil, errors.New("oracle: seed catalog sizes must not be negative")
	case len(cfg.World.AdversarialLedger().Entries()) > 0:
		return nil, errors.New("oracle: seed catalog does not model adversarial drops")
	}
	cfg.MaxEventsPerBlock = cmp.Or(cfg.MaxEventsPerBlock, segment.DefaultMaxEventsPerBlock)
	cfg.MaxSegmentBytes = cmp.Or(cfg.MaxSegmentBytes, maintainer.DefaultMaxSegmentBytes)
	if cfg.Start.IsZero() {
		cfg.Start = seedEpoch
	}
	s := &seeder{cfg: cfg, meta: memstore.New(), clock: cfg.Start}
	s.chain = syncstate.New(s.meta)
	if err := s.backfill(); err != nil {
		return nil, err
	}
	backfillDone := s.clock
	if err := s.live(ctx); err != nil {
		return nil, err
	}
	if err := s.archive(ctx); err != nil {
		return nil, err
	}
	if err := s.finish(ctx, backfillDone); err != nil {
		return nil, err
	}
	return s.out, nil
}

type seeder struct {
	cfg   SeedCatalogConfig
	meta  metastore.Store
	chain *syncstate.StateStore
	clock time.Time
	out   *SeededCatalog
	// touched is every DID a live commit or sync wrote, in first-seen
	// order, for the verifier chain state.
	touched []atmos.DID
}

// tick advances the fixture clock, so every seeded timestamp is distinct
// and deterministic.
func (s *seeder) tick() time.Time {
	s.clock = s.clock.Add(time.Millisecond)
	return s.clock
}

func (s *seeder) backfill() error {
	w := s.cfg.World
	s.out = &SeededCatalog{}
	indices, err := w.AccountIndicesForTest()
	if err != nil {
		return err
	}
	for _, idx := range indices {
		acct, err := w.LoadAccount(idx)
		if err != nil {
			return err
		}
		deleted, err := w.IsAccountDeleted(idx)
		if err != nil {
			return err
		}
		_, unavailable, err := w.RepoUnavailableStatus(idx)
		if err != nil {
			return err
		}
		started := s.tick()
		st := backfill.RepoStatus{
			Backfill:        backfill.RepoBackfillStatus{Status: backfill.StatusUnavailable, StartedAt: started, CompletedAt: s.tick()},
			UpdatedAt:       s.clock,
			LastAttemptedAt: started,
			Active:          !deleted,
		}
		if !deleted && !unavailable {
			_, commit, rows, err := s.repoRows(idx, string(acct.DID), started)
			if err != nil {
				return err
			}
			s.out.Events = append(s.out.Events, rows...)
			st.Backfill.Status = backfill.StatusComplete
			st.Backfill.Rev = commit.Rev
			st.Rev = commit.Rev
		}
		val, err := backfill.EncodeRepoStatus(&st)
		if err != nil {
			return err
		}
		if err := s.meta.Set(context.Background(), backfill.RepoKey(string(acct.DID)), val); err != nil {
			return err
		}
		s.out.Repos++
	}
	s.out.BackfillEvents = len(s.out.Events)
	return nil
}

// repoRows loads the world's head for idx and returns the rows the backfill
// handler writes for it: one create per record in MST walk order, sharing
// the commit rev and one witnessed_at.
func (s *seeder) repoRows(idx int, did string, witnessed time.Time) (*repo.Repo, *repo.Commit, []segment.Event, error) {
	var car bytes.Buffer
	if err := s.cfg.World.ExportRepoCAR(idx, &car); err != nil {
		return nil, nil, nil, fmt.Errorf("oracle: seed export repo %s: %w", did, err)
	}
	r, commit, err := repo.LoadCompleteFromCAR(&car)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("oracle: seed load repo %s: %w", did, err)
	}
	var rows []segment.Event
	err = r.Tree.Walk(func(key string, cid cbor.CID) error {
		collection, rkey := repo.SplitMSTKey(key)
		payload, err := r.Store.GetBlock(cid)
		if err != nil {
			return err
		}
		rows = append(rows, segment.Event{
			WitnessedAt: witnessed.UnixMicro(),
			Kind:        segment.KindCreate,
			DID:         did,
			Collection:  collection,
			Rkey:        rkey,
			Rev:         commit.Rev,
			Payload:     append([]byte(nil), payload...),
		})
		return nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("oracle: seed walk repo %s: %w", did, err)
	}
	return r, commit, rows, nil
}

// live generates the live frames and appends the rows ingest derives from
// each. The derivation reads current world state, so it runs right after
// each frame is generated.
func (s *seeder) live(ctx context.Context) error {
	w := s.cfg.World
	s.out.LiveStartCursor = w.CurrentSeq()
	seen := map[atmos.DID]bool{}
	for range s.cfg.LiveEvents {
		frame, err := w.GenerateOneForTest(ctx)
		if err != nil {
			return fmt.Errorf("oracle: seed generate live frame: %w", err)
		}
		evt, err := decodeOracleFirehoseFrame(frame)
		if err != nil {
			return err
		}
		var did atmos.DID
		switch {
		case evt.Commit != nil:
			did = atmos.DID(evt.Commit.Repo)
		case evt.Sync != nil:
			did = atmos.DID(evt.Sync.DID)
		case evt.Identity != nil:
			s.chain.RecordIdentitySeq(atmos.DID(evt.Identity.DID), evt.Seq)
		case evt.Account != nil:
			return fmt.Errorf("oracle: seed catalog does not model #account frames (seq %d)", evt.Seq)
		}
		if did != "" && !seen[did] {
			seen[did] = true
			s.touched = append(s.touched, did)
		}
		rows, err := expectedSegmentEventsFromFirehoseEvent(w, evt)
		if err != nil {
			return err
		}
		witnessed := s.tick().UnixMicro()
		for i := range rows {
			rows[i].WitnessedAt = witnessed
			rows[i].UpstreamRelayCursor = evt.Seq
		}
		s.out.Events = append(s.out.Events, rows...)
	}
	s.out.RelayCursor = w.CurrentSeq()
	if len(w.AdversarialLedger().Entries()) > 0 {
		return errors.New("oracle: seed catalog does not model adversarial drops")
	}
	return nil
}

// archive commits Events as main blocks, sealing on the maintainer's
// rotation rule. It never seals after the last block, so the active segment
// keeps blocks.
func (s *seeder) archive(ctx context.Context) error {
	cs := s.cfg.Session
	if _, err := cs.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return fmt.Errorf("oracle: seed init namespace: %w", err)
	}
	if len(s.out.Events) == 0 {
		s.out.NextSeq = 1
		return nil
	}
	bb, err := segment.NewBlockBuilder(s.cfg.MaxEventsPerBlock)
	if err != nil {
		return err
	}
	evs := s.out.Events
	var (
		frames  [][]byte
		blocks  []catalog.SealBlock
		framed  int64
		segIdx  uint64
		cursorN = s.out.LiveStartCursor
	)
	for start := 0; start < len(evs); start += s.cfg.MaxEventsPerBlock {
		end := min(start+s.cfg.MaxEventsPerBlock, len(evs))
		for i := start; i < end; i++ {
			evs[i].Seq = uint64(i + 1)
			if _, err := bb.Append(evs[i]); err != nil {
				return fmt.Errorf("oracle: seed encode seq %d: %w", evs[i].Seq, err)
			}
		}
		frame, info := bb.Encode()
		refs, err := s.cfg.Uploader.Upload(ctx, cs, [][]byte{frame})
		if err != nil {
			return fmt.Errorf("oracle: seed upload block: %w", err)
		}
		// relay/cursor names the last upstream seq whose rows are all
		// archived: one before the next live row's event, or the tip once
		// every row is in. Frames that derived no rows are covered too.
		switch {
		case end == len(evs):
			cursorN = s.out.RelayCursor
		case evs[end].UpstreamRelayCursor != 0:
			cursorN = evs[end].UpstreamRelayCursor - 1
		}
		res, err := cs.CommitBlock(ctx, catalog.Block{
			Namespace: catalog.Main,
			Info:      info,
			Object:    refs[0],
			Meta:      []metastore.Op{relayCursorOp(cursorN)},
		})
		if err != nil {
			return fmt.Errorf("oracle: seed commit block [%d,%d]: %w", info.MinSeq, info.MaxSeq, err)
		}
		if res.Segment != segIdx {
			return fmt.Errorf("oracle: seed block landed in segment %d, want %d", res.Segment, segIdx)
		}
		frames = append(frames, frame)
		blocks = append(blocks, catalog.SealBlock{ObjectID: res.ObjectID, CompressedLength: int64(len(frame))})
		framed += 8 + int64(len(frame))
		if end < len(evs) && framed >= s.cfg.MaxSegmentBytes {
			if err := s.seal(ctx, segIdx, frames, blocks); err != nil {
				return err
			}
			segIdx++
			s.out.SealedSegments++
			frames, blocks, framed = nil, nil, 0
		}
	}
	s.out.ActiveBlocks = len(blocks)
	s.out.NextSeq = uint64(len(evs)) + 1
	return nil
}

func (s *seeder) seal(ctx context.Context, segIdx uint64, frames [][]byte, blocks []catalog.SealBlock) error {
	header, footer, h, err := segment.BuildSealed(segment.SliceFrameSource(frames))
	if err != nil {
		return fmt.Errorf("oracle: seed build seal of segment %d: %w", segIdx, err)
	}
	if int(h.BlockCount) != len(blocks) {
		return fmt.Errorf("oracle: seed footer of segment %d indexes %d of %d blocks", segIdx, h.BlockCount, len(blocks))
	}
	refs, err := s.cfg.Uploader.Upload(ctx, s.cfg.Session, [][]byte{footer})
	if err != nil {
		return fmt.Errorf("oracle: seed upload footer of segment %d: %w", segIdx, err)
	}
	_, err = s.cfg.Session.Seal(ctx, catalog.Seal{
		Namespace: catalog.Main,
		Segment:   segIdx,
		Header:    header,
		Footer:    refs[0],
		Blocks:    blocks,
	})
	if err != nil {
		return fmt.Errorf("oracle: seed seal segment %d: %w", segIdx, err)
	}
	return nil
}

// finish stages the remaining metadata in memory, through the same helpers
// local mode writes it with, and copies it into the catalog in key order.
func (s *seeder) finish(ctx context.Context, backfillDone time.Time) error {
	counts, err := backfill.CountStatuses(s.meta)
	if err != nil {
		return err
	}
	if err := backfill.SaveCounts(s.meta, counts); err != nil {
		return err
	}
	if err := lifecycle.WritePhaseWithBackfillTiming(ctx, s.meta, lifecycle.PhaseSteadyState,
		s.tick(), s.cfg.Start, backfillDone); err != nil {
		return err
	}
	for _, did := range s.touched {
		acct, ok, err := s.cfg.World.FindAccountByDID(did)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("oracle: seed live DID %s is not a world account", did)
		}
		_, commit, _, err := s.repoRows(acct.Index, string(did), s.clock)
		if err != nil {
			return err
		}
		if err := s.chain.SaveChain(ctx, did, atmossync.ChainState{Rev: commit.Rev, Data: commit.Data}); err != nil {
			return err
		}
		s.chain.PromoteChain(did, commit.Rev)
	}
	b := s.meta.NewBatch()
	s.chain.StageFlush(b)
	b.Set([]byte(catalog.RelayCursorKey), relayCursorOp(s.out.RelayCursor).Value)
	if err := b.Commit(ctx); err != nil {
		return err
	}

	it, err := s.meta.NewIter(ctx, nil, nil)
	if err != nil {
		return err
	}
	var ops []metastore.Op
	for it.Next() {
		ops = append(ops, metastore.Op{Kind: metastore.OpSet, Key: slices.Clone(it.Key()), Value: slices.Clone(it.Value())})
	}
	if err := errors.Join(it.Err(), it.Close()); err != nil {
		return err
	}
	for chunk := range slices.Chunk(ops, seedMetaChunk) {
		if _, err := s.cfg.Session.CommitMeta(ctx, chunk); err != nil {
			return fmt.Errorf("oracle: seed commit metadata: %w", err)
		}
	}
	return nil
}

func relayCursorOp(cursor int64) metastore.Op {
	return metastore.Op{
		Kind:  metastore.OpSet,
		Key:   []byte(catalog.RelayCursorKey),
		Value: metastore.EncodeVersionedUint64LE(1, uint64(cursor)),
	}
}

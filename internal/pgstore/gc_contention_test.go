package pgstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest"
	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/internal/objstore/protocol"
	"github.com/bluesky-social/jetstream/internal/pgstore"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgtest"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos/streaming"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

const gcContentionLease = 30 * time.Second
const gcContentionWarmup = 2900 * time.Millisecond
const gcContentionBlockEvents = 65_536

type gcContentionConfig struct {
	Rows        int           `json:"Rows"`
	TailPercent int           `json:"TailPercent"`
	Duration    time.Duration `json:"DurationNS"`
	Rate        int           `json:"EventsPerSecond"`
	Mode        string        `json:"PlanCacheMode"`
	Label       string        `json:"Label"`
	Case        string        `json:"Case"`
}

//nolint:paralleltest // The experiment deliberately measures competing work on one PostgreSQL server.
func TestGCCollectorContention(t *testing.T) {
	raw := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_ROWS")
	if raw == "" {
		t.Skip("set JETSTREAM_TEST_GC_CONTENTION_ROWS to run the indexed Collector/ingest experiment")
	}
	cfg := gcContentionConfig{TailPercent: 10, Duration: 15 * time.Second, Rate: 1000, Mode: "auto", Label: "current", Case: "both"}
	var err error
	cfg.Rows, err = strconv.Atoi(raw)
	require.NoError(t, err)
	for name, dest := range map[string]*int{"TAIL_PERCENT": &cfg.TailPercent, "RATE": &cfg.Rate} {
		if value := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_" + name); value != "" {
			*dest, err = strconv.Atoi(value)
			require.NoError(t, err)
		}
	}
	if value := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_DURATION"); value != "" {
		cfg.Duration, err = time.ParseDuration(value)
		require.NoError(t, err)
	}
	if value := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_PLAN_MODE"); value != "" {
		cfg.Mode = value
	}
	if value := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_LABEL"); value != "" {
		cfg.Label = value
	}
	if value := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_CASE"); value != "" {
		cfg.Case = value
	}
	require.GreaterOrEqual(t, cfg.Rows, 1000)
	require.LessOrEqual(t, cfg.Rows, 20_000_000)
	require.GreaterOrEqual(t, cfg.TailPercent, 1)
	require.LessOrEqual(t, cfg.TailPercent, 50)
	require.GreaterOrEqual(t, cfg.Duration, 4*time.Second)
	require.LessOrEqual(t, cfg.Duration, time.Minute)
	require.GreaterOrEqual(t, cfg.Rate, 1)
	require.LessOrEqual(t, cfg.Rate, 20_000)
	offered := gcContentionArrivals(cfg.Duration, cfg.Rate)
	require.LessOrEqual(t, offered, 60_000, "keep every event below the real unfolded cap without folding or dropping closed blocks")
	require.Contains(t, []string{"auto", "force_custom_plan", "force_generic_plan"}, cfg.Mode)
	require.Contains(t, []string{"both", "no_gc", "gc"}, cfg.Case)
	base := os.Getenv("JETSTREAM_TEST_GC_CONTENTION_ARTIFACT_DIR")
	if base != "" {
		require.NoError(t, os.MkdirAll(base, 0700))
	}
	directory, err := os.MkdirTemp(base, "jetstream-gc-contention-")
	require.NoError(t, err)
	t.Logf("GC contention artifacts: %s", directory)
	cases := []string{cfg.Case}
	if cfg.Case == "both" {
		cases = []string{"no_gc", "gc"}
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) { gcContentionRun(t, cfg, name == "gc", directory) })
	}
}

type gcContentionSample struct {
	Due, AppendStart, AppendDone, Committed time.Time
	Dropped                                 bool
}
type gcContentionSummary struct {
	CommittedBeforeEnd int     `json:"CommittedBeforeWindowEnd"`
	BacklogAtEnd       int     `json:"NonDroppedUncommittedAtWindowEnd"`
	Offered            int     `json:"Offered"`
	Accepted           int     `json:"AcceptedEventually"`
	Committed          int     `json:"CommittedEventually"`
	Dropped            int     `json:"Dropped"`
	Pending            int     `json:"AcceptedUncommittedEventually"`
	QueueP99MS         float64 `json:"ScheduleToAppendP99MS"`
	AppendP99MS        float64 `json:"AppendCallP99MS"`
	CommitP50MS        float64 `json:"ScheduleToCommitP50MS"`
	CommitP99MS        float64 `json:"ScheduleToCommitP99MS"`
	CommitMaxMS        float64 `json:"ScheduleToCommitMaxMS"`
}

func gcContentionArrivals(duration time.Duration, rate int) int {
	return int((duration*time.Duration(rate) + time.Second - 1) / time.Second)
}
func TestGCContentionArrivalCount(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		duration   time.Duration
		rate, want int
	}{
		{15 * time.Second, 1000, 15_000},
		{4 * time.Second, 100, 400},
		{4001 * time.Millisecond, 1, 5},
		{time.Nanosecond, 1, 1},
	} {
		require.Equal(t, tc.want, gcContentionArrivals(tc.duration, tc.rate))
	}
}

func gcContentionInterruptedRenew(parentErr, renewErr error) bool {
	return parentErr != nil && (errors.Is(renewErr, context.Canceled) || errors.Is(renewErr, context.DeadlineExceeded))
}
func TestGCContentionRenewShutdown(t *testing.T) {
	t.Parallel()
	require.True(t, gcContentionInterruptedRenew(context.Canceled, fmt.Errorf("renew: %w", context.Canceled)))
	require.False(t, gcContentionInterruptedRenew(nil, context.DeadlineExceeded), "a renewal's own lease deadline is a real failure")
	require.False(t, gcContentionInterruptedRenew(context.Canceled, streaming.ErrNotHolder), "loss of ownership remains visible during shutdown")
	require.False(t, gcContentionInterruptedRenew(context.Canceled, nil), "a successful completed renewal remains an observation")
}

func gcContentionQuantile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	slices.Sort(values)
	return values[min(len(values)-1, int(float64(len(values))*p))]
}
func gcContentionMS(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func gcContentionSummarize(samples []gcContentionSample, from, to time.Time) gcContentionSummary {
	var out gcContentionSummary
	var queue, appendCall, commit []float64
	for _, sample := range samples {
		if sample.Due.Before(from) || !sample.Due.Before(to) {
			continue
		}
		out.Offered++
		if sample.Dropped {
			out.Dropped++
		}
		if !sample.AppendDone.IsZero() {
			out.Accepted++
			queue = append(queue, gcContentionMS(sample.AppendStart.Sub(sample.Due)))
			appendCall = append(appendCall, gcContentionMS(sample.AppendDone.Sub(sample.AppendStart)))
		}
		if !sample.Committed.IsZero() {
			out.Committed++
			if sample.Committed.Before(to) {
				out.CommittedBeforeEnd++
			}
			commit = append(commit, gcContentionMS(sample.Committed.Sub(sample.Due)))
		}
	}
	out.Pending = out.Accepted - out.Committed
	out.BacklogAtEnd = out.Offered - out.Dropped - out.CommittedBeforeEnd
	out.QueueP99MS = gcContentionQuantile(queue, .99)
	out.AppendP99MS = gcContentionQuantile(appendCall, .99)
	out.CommitP50MS = gcContentionQuantile(commit, .5)
	out.CommitP99MS = gcContentionQuantile(commit, .99)
	out.CommitMaxMS = gcContentionQuantile(commit, 1)
	return out
}
func TestGCContentionAccounting(t *testing.T) {
	t.Parallel()
	base := time.Unix(100, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	samples := []gcContentionSample{
		{Due: at(100), AppendStart: at(105), AppendDone: at(110), Committed: at(500)},
		{Due: at(200), Dropped: true},
		{Due: at(300), AppendStart: at(305), AppendDone: at(310)},
		{Due: at(400), AppendStart: at(405), AppendDone: at(410), Committed: at(500)},
	}
	got := gcContentionSummarize(samples, at(100), at(400))
	require.Equal(t, 3, got.Offered)
	require.Equal(t, 2, got.Accepted)
	require.Equal(t, 1, got.Committed)
	require.Equal(t, 1, got.Dropped)
	require.Equal(t, 1, got.Pending)
	require.Equal(t, float64(400), got.CommitP99MS)
	require.Zero(t, got.CommittedBeforeEnd)
	require.Equal(t, 2, got.BacklogAtEnd)
}

type gcContentionEvent struct {
	DueMS         float64  `json:"DueMS"`
	AppendStartMS *float64 `json:"AppendStartMS,omitempty"`
	AppendDoneMS  *float64 `json:"AppendDoneMS,omitempty"`
	CommittedMS   *float64 `json:"CommittedMS,omitempty"`
	Dropped       bool     `json:"Dropped"`
}
type gcContentionTxn struct {
	Kind     string   `json:"Kind"`
	BeginMS  float64  `json:"BeginCallMS"`
	FenceMS  *float64 `json:"FenceResponseMS,omitempty"`
	CommitMS float64  `json:"CommitCallMS"`
	TotalMS  float64  `json:"TotalClientMS"`
	Error    string   `json:"Error,omitempty"`
}
type gcContentionRenew struct {
	StartMS    float64 `json:"StartMS"`
	DurationMS float64 `json:"DurationMS"`
	Error      string  `json:"Error,omitempty"`
}
type gcContentionSweep struct {
	Enabled               bool    `json:"Enabled"`
	StartMS               float64 `json:"StartMS"`
	FinishMS              float64 `json:"FinishMS"`
	Marked                int     `json:"Marked"`
	Claimed               int     `json:"Claimed"`
	Deleted               int     `json:"Deleted"`
	Error                 string  `json:"Error,omitempty"`
	HistogramSeconds      float64 `json:"HistogramSecondsExcludingGaugeRefresh"`
	RenewCallsOverlapping int     `json:"RenewCallsOverlapping"`
}
type gcContentionReport struct {
	HotBatchFallbacks  uint64                `json:"HotBatchCommitFallbacks"`
	Database           string                `json:"Database"`
	SeedBackendPID     uint32                `json:"SeedBackendPID"`
	StartedUTC         time.Time             `json:"StartedUTC"`
	Before             gcContentionState     `json:"Before"`
	After              gcContentionState     `json:"After"`
	ExpectedMarked     int                   `json:"ExpectedMarked"`
	ExpectedClaimed    int                   `json:"ExpectedClaimed"`
	GaugeObjects       int                   `json:"GaugeObjects"`
	Config             gcContentionConfig    `json:"Config"`
	GC                 bool                  `json:"GC"`
	ServerVersion      string                `json:"ServerVersion"`
	Settings           map[string]string     `json:"Settings"`
	Index              string                `json:"DeployedIndex"`
	Scope              string                `json:"Scope"`
	LeaseSeconds       int                   `json:"LeaseSeconds"`
	RenewalSeconds     int                   `json:"RenewalSeconds"`
	MaxConns           int32                 `json:"MaxConns"`
	BlockEvents        int                   `json:"BlockEvents"`
	BlockMaxAgeSeconds int                   `json:"BlockMaxAgeSeconds"`
	Window             gcContentionSummary   `json:"FixedIngressWindow"`
	DuringSweep        *gcContentionSummary  `json:"EventsScheduledDuringSweep,omitempty"`
	PerSecond          []gcContentionSummary `json:"PerSecond"`
	Events             []gcContentionEvent   `json:"Events"`
	Transactions       []gcContentionTxn     `json:"Transactions"`
	Renewals           []gcContentionRenew   `json:"Renewals"`
	Sweep              gcContentionSweep     `json:"Sweep"`
	WALBytes           uint64                `json:"ServerGlobalWALBytes"`
	DrainMS            float64               `json:"DrainDurabilityCallMS"`
	CompletionMS       float64               `json:"CompletionMS"`
	MaxQueueDepth      int                   `json:"MaxIngressQueueDepth"`
	QueueCapacity      int                   `json:"IngressQueueCapacity"`
	Errors             []string              `json:"Errors,omitempty"`
}
type gcContentionBook struct {
	sync.Mutex
	samples      []gcContentionSample
	bySeq        []int
	committed    uint64
	transactions []gcContentionTxn
	err          error
}

func (b *gcContentionBook) acknowledge(next uint64) {
	b.Lock()
	defer b.Unlock()
	if next > uint64(len(b.bySeq))+1 {
		b.err = fmt.Errorf("commit next %d exceeds assigned events %d", next, len(b.bySeq))
		return
	}
	now := time.Now()
	for b.committed+1 < next {
		b.samples[b.bySeq[b.committed]].Committed = now
		b.committed++
	}
}

type gcContentionDB struct {
	catalog.DB
	book *gcContentionBook
}

func (d gcContentionDB) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	start := time.Now()
	tx, err := d.DB.Begin(ctx, kind)
	if err != nil {
		return nil, err
	}
	return &gcContentionTx{Tx: tx, book: d.book, start: start, observation: gcContentionTxn{Kind: string(kind), BeginMS: gcContentionMS(time.Since(start))}}, nil
}

type gcContentionTx struct {
	catalog.Tx
	book        *gcContentionBook
	start       time.Time
	observation gcContentionTxn
	recorded    bool
}

func (tx *gcContentionTx) FenceBump(ctx context.Context, epoch uint64, reads ...catalog.Read) (uint64, bool, error) {
	start := time.Now()
	rev, ok, err := tx.Tx.FenceBump(ctx, epoch, reads...)
	elapsed := gcContentionMS(time.Since(start))
	tx.observation.FenceMS = &elapsed
	return rev, ok, err
}
func (tx *gcContentionTx) record(err error) {
	if tx.recorded {
		return
	}
	tx.recorded = true
	tx.observation.TotalMS = gcContentionMS(time.Since(tx.start))
	if err != nil {
		tx.observation.Error = err.Error()
	}
	tx.book.Lock()
	defer tx.book.Unlock()
	tx.book.transactions = append(tx.book.transactions, tx.observation)
}
func (tx *gcContentionTx) Commit(ctx context.Context) error {
	start := time.Now()
	err := tx.Tx.Commit(ctx)
	tx.observation.CommitMS = gcContentionMS(time.Since(start))
	tx.record(err)
	return err
}
func (tx *gcContentionTx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	tx.record(err)
	return err
}

type gcContentionSink struct {
	sync.Mutex
	closed int
}

func (s *gcContentionSink) BlockClosed(ingest.ClosedBlock) { s.Lock(); defer s.Unlock(); s.closed++ }
func (*gcContentionSink) Rotate(context.Context) error {
	return errors.New("contention experiment does not fold or rotate")
}

func gcContentionSeed(t *testing.T, store *pgstore.Store, cfg gcContentionConfig) (marked, claimed int) {
	t.Helper()
	base := cfg.Rows - cfg.Rows*cfg.TailPercent/100
	_, err := store.Pool().Exec(t.Context(), `INSERT INTO objects(object_id,key,sha256,byte_length,state,created_at,unreferenced_at)
 SELECT id,md5('key-'||id)::uuid,decode(md5('sha-'||id)||md5('sha2-'||id),'hex'),1000,
 CASE WHEN id>$2 AND id%8 IN (1,3) THEN 'uploading' ELSE 'available' END,
 now()-CASE WHEN id>$2 AND id%8=3 THEN interval '10 minutes' ELSE interval '2 days' END,
 CASE WHEN id>$2 AND id%8=0 THEN now()-interval '2 days' WHEN id>$2 AND id%8=2 THEN now()-interval '10 minutes' ELSE NULL END
 FROM generate_series(1,$1::bigint)id`, cfg.Rows, base)
	require.NoError(t, err)
	_, err = store.Pool().Exec(t.Context(), fmt.Sprintf(`
 INSERT INTO segments(namespace,segment_index,state,current_generation_id,revision) VALUES('bootstrap_live',0,'sealed',1,0);
 INSERT INTO segment_generations(generation_id,namespace,segment_index,header,footer_object_id,revision) VALUES(1,'bootstrap_live',0,decode(repeat('00',256),'hex'),1,0);
 INSERT INTO generation_blocks(generation_id,ordinal,object_id,compressed_length)
 SELECT 1,(row_number() OVER(ORDER BY object_id)-1)::integer,object_id,1000 FROM objects WHERE object_id<>1 AND (object_id<=%d OR object_id%%8>=5);
 CREATE INDEX objects_gc_claim_candidates_id ON objects(object_id) WHERE state='uploading' OR (state='available' AND unreferenced_at IS NOT NULL);
 ANALYZE;`, base))
	require.NoError(t, err)
	for id := base + 1; id <= cfg.Rows; id++ {
		if id%8 == 4 {
			marked++
		}
		if id%8 == 0 || id%8 == 1 {
			claimed++
		}
	}
	return marked, claimed
}

func gcContentionRun(t *testing.T, cfg gcContentionConfig, withGC bool, directory string) {
	t.Helper()
	parsed, err := url.Parse(pgtest.NewDatabase(t))
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("plan_cache_mode", cfg.Mode)
	parsed.RawQuery = query.Encode()
	store, err := pgstore.Open(t.Context(), pgstore.Config{URL: parsed.String(), MaxConns: 32})
	require.NoError(t, err)
	t.Cleanup(store.Close)
	archiveID := [16]byte{9}
	require.NoError(t, store.Initialize(t.Context(), archiveID))
	expectedMark, expectedClaim := gcContentionSeed(t, store, cfg)
	lease := store.NewLease()
	acquiredAt := time.Now()
	require.NoError(t, lease.Acquire(t.Context(), gcContentionLease))
	book := &gcContentionBook{}
	catalogMetrics := catalog.NewMetrics(prometheus.NewRegistry())
	session := catalog.NewSession(catalog.SessionConfig{DB: gcContentionDB{DB: store, book: book}, Epoch: lease.Epoch(), Metrics: catalogMetrics})
	_, err = session.InitNamespace(t.Context(), catalog.Main, nil)
	require.NoError(t, err)
	conn, err := store.Pool().Acquire(t.Context())
	require.NoError(t, err)
	version, settings := gcPlanConnectionInfo(t, conn.Conn())
	seedPID := conn.Conn().PgConn().PID()
	conn.Release()
	require.Equal(t, cfg.Mode, settings["plan_cache_mode"])
	report := gcContentionReport{Database: parsed.Path[1:], SeedBackendPID: seedPID, Config: cfg, GC: withGC, ServerVersion: version, Settings: settings, LeaseSeconds: 30, RenewalSeconds: 1, MaxConns: 32, BlockEvents: gcContentionBlockEvents, BlockMaxAgeSeconds: 300,
		Scope: "Real PostgreSQL, catalog Session, inline hot ingest and full Collector. Synthetic sealed history has no decodable segment payload. Fixture blob keys are absent, so deletes exercise idempotent missing-key success. No maintainer, folding, follower or S3 latency; offered events stay below the unfolded cap.", QueueCapacity: min(cfg.Rate, 4096)}
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indexrelid='objects_gc_claim_candidates_id'::regclass`).Scan(&report.Index))
	gcMetrics := protocol.NewGCMetrics(prometheus.NewRegistry())
	collector, err := protocol.NewCollector(protocol.GCConfig{Blob: memblob.New(), ArchiveID: archiveID, Delay: 6 * time.Hour, OrphanAge: time.Hour, Concurrency: 8, Metrics: gcMetrics})
	require.NoError(t, err)
	report.Before = gcContentionReadState(t, store, cfg)
	report.ExpectedMarked, report.ExpectedClaimed = expectedMark, expectedClaim
	sink := &gcContentionSink{}
	current := 0
	writer, err := ingest.Open(ingest.Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxEventsPerBlock: gcContentionBlockEvents,
		OnAppend: func(ev *segment.Event) error {
			book.Lock()
			defer book.Unlock()
			if ev.Seq != uint64(len(book.bySeq))+1 {
				return fmt.Errorf("assigned seq %d after %d events", ev.Seq, len(book.bySeq))
			}
			book.bySeq = append(book.bySeq, current)
			return nil
		},
		OnDurableBatch: func(_ context.Context, _ metastore.Batch, next uint64, _ bool, _ any) (func(), func(error), error) {
			return func() { book.acknowledge(next) }, nil, nil
		},
		Hot: &ingest.HotConfig{Session: session, Sink: sink, BlockMaxAge: 5 * time.Minute, MaxUnfoldedEvents: ingest.DefaultMaxUnfoldedEvents}})
	require.NoError(t, err)
	closeWriter := sync.OnceValue(writer.Close)
	t.Cleanup(func() { require.NoError(t, closeWriter()) })
	ctx, cancel := context.WithTimeout(t.Context(), cfg.Duration+90*time.Second)
	defer cancel()
	walBefore, err := pgfixture.WALPosition(ctx, store)
	require.NoError(t, err)
	book.transactions = nil
	start := time.Now()
	report.StartedUTC = start.UTC()
	name := "no_gc"
	if withGC {
		name = "gc"
	}
	require.NoError(t, os.WriteFile(filepath.Join(directory, name+"-database.txt"), []byte(report.Database+"\n"), 0600))
	count := gcContentionArrivals(cfg.Duration, cfg.Rate)
	book.samples = make([]gcContentionSample, count)
	for i := range book.samples {
		book.samples[i].Due = start.Add(time.Duration(i) * time.Second / time.Duration(cfg.Rate))
	}
	queue := make(chan int, report.QueueCapacity)
	appendDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				appendDone <- ctx.Err()
				return
			case i, ok := <-queue:
				if !ok {
					appendDone <- nil
					return
				}
				current = i
				ev := segment.Event{Kind: segment.KindCreate, DID: "did:plc:abcdefghijklmnopqrstuvwx", Collection: "app.bsky.feed.post", Rkey: strconv.Itoa(i), Rev: "3l", Payload: []byte{0xa1, 0x64, 't', 'e', 'x', 't', 0x61, 'x'}, WitnessedAt: book.samples[i].Due.UnixMicro()}
				book.Lock()
				book.samples[i].AppendStart = time.Now()
				book.Unlock()
				if err := writer.Append(ctx, &ev); err != nil {
					appendDone <- err
					return
				}
				book.Lock()
				book.samples[i].AppendDone = time.Now()
				book.Unlock()
			}
		}
	}()
	renewDone := make(chan []gcContentionRenew, 1)
	go func() {
		var observations []gcContentionRenew
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastOK := acquiredAt
		for {
			select {
			case <-ctx.Done():
				renewDone <- observations
				return
			case <-ticker.C:
			}
			callStart := time.Now()
			callCtx, callCancel := context.WithDeadline(ctx, lastOK.Add(gcContentionLease))
			renewErr := lease.Renew(callCtx, gcContentionLease)
			callCancel()
			if gcContentionInterruptedRenew(ctx.Err(), renewErr) {
				renewDone <- observations
				return
			}
			observation := gcContentionRenew{StartMS: gcContentionMS(callStart.Sub(start)), DurationMS: gcContentionMS(time.Since(callStart))}
			if renewErr == nil {
				lastOK = callStart
			} else {
				observation.Error = renewErr.Error()
			}
			observations = append(observations, observation)
			if errors.Is(renewErr, streaming.ErrNotHolder) || time.Now().After(lastOK.Add(gcContentionLease)) {
				cancel()
				renewDone <- observations
				return
			}
		}
	}()
	sweepDone := make(chan gcContentionSweep, 1)
	go func() {
		sweep := gcContentionSweep{Enabled: withGC}
		if !withGC {
			sweepDone <- sweep
			return
		}
		timer := time.NewTimer(time.Until(start.Add(gcContentionWarmup)))
		defer timer.Stop()
		select {
		case <-ctx.Done():
			sweep.Error = ctx.Err().Error()
			sweepDone <- sweep
			return
		case <-timer.C:
		}
		sweep.StartMS = gcContentionMS(time.Since(start))
		result, sweepErr := collector.Run(ctx, session, store)
		sweep.FinishMS = gcContentionMS(time.Since(start))
		sweep.Marked, sweep.Claimed, sweep.Deleted = result.Marked, result.Claimed, result.Deleted
		if sweepErr != nil {
			sweep.Error = sweepErr.Error()
			cancel()
		}
		sweepDone <- sweep
	}()
	// The producer follows absolute arrival times independently of PostgreSQL or Append completion.
	ticker := time.NewTicker(time.Millisecond)
	next := 0
	scheduling := true
	for scheduling {
		select {
		case <-ctx.Done():
			scheduling = false
		case <-ticker.C:
			now := time.Now()
			for next < count && !book.samples[next].Due.After(now) {
				select {
				case queue <- next:
				default:
					book.Lock()
					book.samples[next].Dropped = true
					book.Unlock()
				}
				report.MaxQueueDepth = max(report.MaxQueueDepth, len(queue))
				next++
			}
			if !now.Before(start.Add(cfg.Duration)) {
				scheduling = false
			}
		}
	}
	ticker.Stop()
	close(queue)
	if appendErr := <-appendDone; appendErr != nil {
		report.Errors = append(report.Errors, "append: "+appendErr.Error())
	}
	drainStart := time.Now()
	if drainErr := writer.DrainDurability(ctx); drainErr != nil {
		report.Errors = append(report.Errors, "drain: "+drainErr.Error())
	}
	report.DrainMS = gcContentionMS(time.Since(drainStart))
	report.Sweep = <-sweepDone
	if closeErr := closeWriter(); closeErr != nil {
		report.Errors = append(report.Errors, "close: "+closeErr.Error())
	}
	report.CompletionMS = gcContentionMS(time.Since(start))
	cancel()
	report.Renewals = <-renewDone
	walAfter, walErr := pgfixture.WALPosition(t.Context(), store)
	if walErr != nil {
		report.Errors = append(report.Errors, "WAL: "+walErr.Error())
	} else {
		report.WALBytes = walAfter - walBefore
	}
	report.Window = gcContentionSummarize(book.samples, start, start.Add(cfg.Duration))
	if withGC {
		summary := gcContentionSummarize(book.samples, start.Add(time.Duration(report.Sweep.StartMS*float64(time.Millisecond))), start.Add(time.Duration(report.Sweep.FinishMS*float64(time.Millisecond))))
		report.DuringSweep = &summary
		for _, renew := range report.Renewals {
			if renew.StartMS < report.Sweep.FinishMS && renew.StartMS+renew.DurationMS >= report.Sweep.StartMS {
				report.Sweep.RenewCallsOverlapping++
			}
		}
		var metric dto.Metric
		require.NoError(t, gcMetrics.RunDuration.Write(&metric))
		report.Sweep.HistogramSeconds = metric.GetHistogram().GetSampleSum()
	}
	for offset := time.Duration(0); offset < cfg.Duration; offset += time.Second {
		report.PerSecond = append(report.PerSecond, gcContentionSummarize(book.samples, start.Add(offset), start.Add(min(offset+time.Second, cfg.Duration))))
	}
	for _, sample := range book.samples {
		event := gcContentionEvent{DueMS: gcContentionMS(sample.Due.Sub(start)), Dropped: sample.Dropped}
		for _, field := range []struct {
			at   time.Time
			dest **float64
		}{{sample.AppendStart, &event.AppendStartMS}, {sample.AppendDone, &event.AppendDoneMS}, {sample.Committed, &event.CommittedMS}} {
			if !field.at.IsZero() {
				value := gcContentionMS(field.at.Sub(start))
				*field.dest = &value
			}
		}
		report.Events = append(report.Events, event)
	}
	report.Transactions = book.transactions
	if book.err != nil {
		report.Errors = append(report.Errors, book.err.Error())
	}
	var fallbackMetric dto.Metric
	require.NoError(t, catalogMetrics.Fallbacks.WithLabelValues(string(catalog.TxHotBatch)).Write(&fallbackMetric))
	report.HotBatchFallbacks = uint64(fallbackMetric.GetCounter().GetValue())
	report.After = gcContentionReadState(t, store, cfg)
	if withGC {
		for _, state := range []string{"available", "uploading", "deleting"} {
			var metric dto.Metric
			require.NoError(t, gcMetrics.Objects.WithLabelValues(state).Write(&metric))
			report.GaugeObjects += int(metric.GetGauge().GetValue())
		}
	}
	data, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	path := filepath.Join(directory, name+".json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	t.Logf("artifact=%s offered=%d committed=%d schedule→commit p99=%.2fms sweep=%.2fms renewal-overlap=%d", path, report.Window.Offered, report.Window.Committed, report.Window.CommitP99MS, report.Sweep.FinishMS-report.Sweep.StartMS, report.Sweep.RenewCallsOverlapping)
	require.Empty(t, report.Errors)
	require.Empty(t, report.Sweep.Error, "full sweep must complete within the bound")
	require.Equal(t, count, next, "all arrival slots must be offered")
	require.Equal(t, count, report.Window.Accepted+report.Window.Dropped)
	require.Zero(t, report.Window.Dropped, "drops invalidate the latency comparison; preserve the artifact for diagnosis")
	require.Equal(t, report.Window.Accepted, report.Window.Committed)
	require.Zero(t, report.Window.Pending)
	require.NoError(t, session.Err())
	require.Zero(t, sink.closed, "the experiment must never pretend an unfolded block was folded")
	require.GreaterOrEqual(t, len(report.Renewals), 2, "exercise real lease renewals")
	var liveLease bool
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SELECT lease_expires_at>now() AND writer_epoch=$1 FROM archive WHERE id=1`, lease.Epoch()).Scan(&liveLease))
	require.True(t, liveLease)
	if withGC {
		require.LessOrEqual(t, report.Sweep.FinishMS, gcContentionMS(cfg.Duration), "the entire sweep must complete while fixed-rate ingress is active")
		require.Equal(t, expectedMark, report.Sweep.Marked)
		require.Equal(t, expectedClaim, report.Sweep.Claimed)
		require.Equal(t, expectedClaim, report.Sweep.Deleted)
	}
	wantObjects, wantMarked := cfg.Rows, 0
	if withGC {
		wantObjects -= expectedClaim
		wantMarked = expectedMark
		require.Equal(t, wantObjects, report.GaugeObjects)
	}
	require.Equal(t, wantObjects, report.After.Objects)
	require.Equal(t, wantMarked, report.After.NewMarks)
	require.Zero(t, report.After.Deleting)
	require.Zero(t, report.After.ReferencedMarked)
	require.Equal(t, report.Before.YoungAvailable, report.After.YoungAvailable)
	require.Equal(t, report.Before.YoungUploading, report.After.YoungUploading)
	require.Equal(t, report.Before.ReferenceRows, report.After.ReferenceRows)
	if withGC {
		require.Zero(t, report.After.OldEligible)
	} else {
		require.Equal(t, report.Before.OldEligible, report.After.OldEligible)
	}
	gcContentionCheckEvents(t, store, book)
}

func gcContentionCheckEvents(t *testing.T, store *pgstore.Store, book *gcContentionBook) {
	t.Helper()
	read, err := store.BeginRead(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, read.Close(context.WithoutCancel(t.Context()))) }()
	rows, err := read.HotBatches(t.Context(), 0)
	require.NoError(t, err)
	var seq uint64 = 1
	for _, row := range rows {
		require.Equal(t, seq, row.FirstSeq)
		require.True(t, row.Inline)
		events, err := segment.DecodeBlockFrame(row.Frame)
		require.NoError(t, err)
		require.Equal(t, int(row.EventCount), len(events))
		for _, event := range events {
			require.Equal(t, seq, event.Seq)
			require.Equal(t, strconv.Itoa(book.bySeq[seq-1]), event.Rkey)
			require.Equal(t, []byte{0xa1, 0x64, 't', 'e', 'x', 't', 0x61, 'x'}, event.Payload)
			require.Equal(t, segment.KindCreate, event.Kind)
			require.Equal(t, "did:plc:abcdefghijklmnopqrstuvwx", event.DID)
			require.Equal(t, "app.bsky.feed.post", event.Collection)
			require.Equal(t, "3l", event.Rev)
			require.Equal(t, book.samples[book.bySeq[seq-1]].Due.UnixMicro(), event.WitnessedAt)
			seq++
		}
		require.Equal(t, row.LastSeq+1, seq)
	}
	require.Equal(t, book.committed+1, seq)
	metadata, err := read.MetaGet(t.Context(), [][]byte{[]byte(catalog.MainSeqKey)})
	require.NoError(t, err)
	require.Equal(t, catalog.EncodeSeq(seq), metadata[catalog.MainSeqKey])
}

type gcContentionState struct {
	Objects          int `json:"Objects"`
	NewMarks         int `json:"NewMarks"`
	Deleting         int `json:"Deleting"`
	ReferencedMarked int `json:"ReferencedMarked"`
	YoungAvailable   int `json:"YoungAvailable"`
	YoungUploading   int `json:"YoungUploading"`
	OldEligible      int `json:"OldEligible"`
	ReferenceRows    int `json:"GenerationReferenceRows"`
}

func gcContentionReadState(t *testing.T, store *pgstore.Store, cfg gcContentionConfig) gcContentionState {
	t.Helper()
	var state gcContentionState
	require.NoError(t, store.Pool().QueryRow(t.Context(), `SELECT count(*),
 count(*) FILTER(WHERE unreferenced_at IS NOT NULL AND object_id>$1 AND object_id%8=4 AND state='available'),
 count(*) FILTER(WHERE state='deleting'),
 count(*) FILTER(WHERE unreferenced_at IS NOT NULL AND (object_id<=$1 OR object_id%8>=5)),
 count(*) FILTER(WHERE object_id>$1 AND object_id%8=2 AND state='available'),
 count(*) FILTER(WHERE object_id>$1 AND object_id%8=3 AND state='uploading'),
 count(*) FILTER(WHERE (state='available' AND unreferenced_at<now()-interval '6 hours') OR (state='uploading' AND created_at<now()-interval '1 hour')),
 (SELECT count(*) FROM generation_blocks) FROM objects`, cfg.Rows-cfg.Rows*cfg.TailPercent/100).Scan(&state.Objects, &state.NewMarks, &state.Deleting, &state.ReferencedMarked, &state.YoungAvailable, &state.YoungUploading, &state.OldEligible, &state.ReferenceRows))
	return state
}

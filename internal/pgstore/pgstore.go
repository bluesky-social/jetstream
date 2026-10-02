// Package pgstore is the PostgreSQL catalog backend (design §8, §9): the
// connection pool, the embedded schema migration, the schema and format
// version check, the writer lease (§6.2), and catalog.DB over SQL. Every
// catalog.Tx primitive is one SQL statement against the §8 schema (plan D1);
// the correctness checks live in the catalog scripts, which run unchanged
// against storagefake.
//
// A leader transaction that fails, or whose COMMIT result is unknown,
// returns an error wrapping catalog.ErrSessionEnded and is never retried
// (design §9.1).
//
// The connection URL carries a password. Nothing in this package logs it or
// puts it in an error.
package pgstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/obs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// Session settings for every connection (design §9.1: transactions stay
// short). A leader transaction that trips one fails, which ends the session.
const (
	statementTimeout       = 10 * time.Second
	lockTimeout            = 5 * time.Second
	idleInTxSessionTimeout = 30 * time.Second
)

// DefaultMaxConns is JETSTREAM_PG_MAX_CONNS's default (design §18). A leader
// runs the live verifier's per-commit chain reads (up to 32 at once), the
// backfill's batched metadata reads and group commits, and archive serving
// reads concurrently; 32 keeps those from queueing on the pool while staying
// far below what PostgreSQL handles comfortably per pod.
const DefaultMaxConns = 32

// Pool tuning (design §9.1 keeps transactions short, so connections turn
// over quickly and reuse is high). Over a WAN link a new connection costs a
// TCP, TLS, and auth handshake — several round trips — so the pool keeps a
// warm floor of idle connections instead of opening them on a burst, and
// recycles old connections with jitter so they do not all reconnect at once.
const (
	// One minIdleFraction of MaxConns stays connected and idle. Busier
	// stretches keep their working set warm anyway (maxConnIdleTime); the
	// floor covers the burst after a quiet period.
	minIdleFraction       = 8
	maxConnLifetime       = time.Hour
	maxConnLifetimeJitter = 10 * time.Minute
	maxConnIdleTime       = 30 * time.Minute
	healthCheckPeriod     = 30 * time.Second
)

// Config configures Open.
type Config struct {
	// URL is a libpq connection string or URL. It holds a password: never
	// log it.
	URL string
	// MaxConns caps the pool. Zero means DefaultMaxConns.
	MaxConns int32
	// Metrics may be nil.
	Metrics *Metrics
}

// String describes the config with the password redacted.
func (c Config) String() string {
	return fmt.Sprintf("pgstore.Config{URL:%q MaxConns:%d}", RedactURL(c.URL), c.MaxConns)
}

// GoString redacts %#v the same way.
func (c Config) GoString() string { return c.String() }

// LogValue implements slog.LogValuer with the password redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Store is a connection pool to one archive's database.
type Store struct {
	pool    *pgxpool.Pool
	connCfg *pgx.ConnConfig // for the dedicated LISTEN connection
	metrics *Metrics
}

var (
	_ catalog.DB       = (*Store)(nil)
	_ catalog.Listener = (*Store)(nil)
)

// ParseConfig parses a connection URL. Its error never contains the URL:
// pgx redacts the password, but a malformed URL can put it anywhere.
func ParseConfig(url string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("pgstore: cannot parse the PostgreSQL connection URL")
	}
	rp := cfg.ConnConfig.RuntimeParams
	rp["statement_timeout"] = durationParam(statementTimeout)
	rp["lock_timeout"] = durationParam(lockTimeout)
	rp["idle_in_transaction_session_timeout"] = durationParam(idleInTxSessionTimeout)
	if rp["application_name"] == "" {
		rp["application_name"] = "jetstream"
	}
	return cfg, nil
}

func durationParam(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// Open connects and pings. It does not check the schema: call CheckVersions.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	pcfg, err := ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	tunePool(pcfg, cfg.MaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	cfg.Metrics.watchPool(pool)
	return &Store{pool: pool, connCfg: pcfg.ConnConfig.Copy(), metrics: cfg.Metrics}, nil
}

// tunePool applies the pool tuning above. maxConns <= 0 means
// DefaultMaxConns.
func tunePool(pcfg *pgxpool.Config, maxConns int32) {
	pcfg.MaxConns = maxConns
	if pcfg.MaxConns <= 0 {
		pcfg.MaxConns = DefaultMaxConns
	}
	pcfg.MinIdleConns = max(1, pcfg.MaxConns/minIdleFraction)
	pcfg.MinConns = pcfg.MinIdleConns
	pcfg.MaxConnLifetime = maxConnLifetime
	pcfg.MaxConnLifetimeJitter = maxConnLifetimeJitter
	pcfg.MaxConnIdleTime = maxConnIdleTime
	pcfg.HealthCheckPeriod = healthCheckPeriod
}

// Close closes the pool.
func (s *Store) Close() { s.pool.Close() }

// Pool returns the underlying pool, for sibling backends (metastore/pg).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Metrics owns the PostgreSQL series (design §23). A nil *Metrics is valid
// and records nothing.
type Metrics struct {
	TxnDuration      *prometheus.HistogramVec
	TxnErrors        *prometheus.CounterVec
	ListenReconnects prometheus.Counter

	pool poolCollector
}

// NewMetrics registers the series against reg. Construct exactly once per
// process.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		TxnDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "jetstream", Subsystem: "pg", Name: "txn_duration_seconds",
			Help:    "PostgreSQL transaction duration from BEGIN to COMMIT or ROLLBACK, by kind.",
			Buckets: obs.LatencyBucketsFast,
		}, []string{"kind"}),
		TxnErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "jetstream", Subsystem: "pg", Name: "txn_errors_total",
			Help: "PostgreSQL transactions that failed or whose commit result is unknown, by kind.",
		}, []string{"kind"}),
		ListenReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "jetstream", Subsystem: "pg", Name: "listen_reconnects_total",
			Help: "Times the LISTEN jetstream_catalog connection was lost and re-established.",
		}),
	}
	reg.MustRegister(m.TxnDuration, m.TxnErrors, m.ListenReconnects, &m.pool)
	return m
}

// watchPool exports pool's statistics. A process opens one pool per Metrics;
// a later pool replaces an earlier one.
func (m *Metrics) watchPool(pool *pgxpool.Pool) {
	if m != nil {
		m.pool.pool.Store(pool)
	}
}

// poolCollector reads pgxpool statistics at scrape time.
type poolCollector struct {
	pool atomic.Pointer[pgxpool.Pool]
}

var (
	poolConnsDesc = prometheus.NewDesc("jetstream_pg_pool_conns",
		"PostgreSQL pool connections by state (acquired, idle, constructing) and the max.", []string{"state"}, nil)
	poolAcquiresDesc = prometheus.NewDesc("jetstream_pg_pool_acquires_total",
		"PostgreSQL pool acquires by outcome: immediate (an idle connection was ready), waited (none was), canceled.", []string{"outcome"}, nil)
	poolAcquireSecondsDesc = prometheus.NewDesc("jetstream_pg_pool_acquire_wait_seconds_total",
		"Total time acquires spent waiting for a connection because none was idle.", nil, nil)
	poolNewConnsDesc = prometheus.NewDesc("jetstream_pg_pool_new_conns_total",
		"PostgreSQL connections the pool opened.", nil, nil)
	poolDestroyedDesc = prometheus.NewDesc("jetstream_pg_pool_destroyed_conns_total",
		"PostgreSQL connections the pool closed for age, by reason (max_lifetime, max_idle).", []string{"reason"}, nil)
)

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- poolConnsDesc
	ch <- poolAcquiresDesc
	ch <- poolAcquireSecondsDesc
	ch <- poolNewConnsDesc
	ch <- poolDestroyedDesc
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	pool := c.pool.Load()
	if pool == nil {
		return
	}
	st := pool.Stat()
	gauge := func(v int32, state string) {
		ch <- prometheus.MustNewConstMetric(poolConnsDesc, prometheus.GaugeValue, float64(v), state)
	}
	gauge(st.AcquiredConns(), "acquired")
	gauge(st.IdleConns(), "idle")
	gauge(st.ConstructingConns(), "constructing")
	gauge(st.MaxConns(), "max")
	waited := st.EmptyAcquireCount()
	ch <- prometheus.MustNewConstMetric(poolAcquiresDesc, prometheus.CounterValue, float64(st.AcquireCount()-waited), "immediate")
	ch <- prometheus.MustNewConstMetric(poolAcquiresDesc, prometheus.CounterValue, float64(waited), "waited")
	ch <- prometheus.MustNewConstMetric(poolAcquiresDesc, prometheus.CounterValue, float64(st.CanceledAcquireCount()), "canceled")
	ch <- prometheus.MustNewConstMetric(poolAcquireSecondsDesc, prometheus.CounterValue, st.EmptyAcquireWaitTime().Seconds())
	ch <- prometheus.MustNewConstMetric(poolNewConnsDesc, prometheus.CounterValue, float64(st.NewConnsCount()))
	ch <- prometheus.MustNewConstMetric(poolDestroyedDesc, prometheus.CounterValue, float64(st.MaxLifetimeDestroyCount()), "max_lifetime")
	ch <- prometheus.MustNewConstMetric(poolDestroyedDesc, prometheus.CounterValue, float64(st.MaxIdleDestroyCount()), "max_idle")
}

func (m *Metrics) observe(kind string, start time.Time, failed bool) {
	if m == nil {
		return
	}
	m.TxnDuration.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	if failed {
		m.TxnErrors.WithLabelValues(kind).Inc()
	}
}

func (m *Metrics) reconnected() {
	if m != nil {
		m.ListenReconnects.Inc()
	}
}

var tracer = obs.Tracer("pgstore")

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

// DefaultMaxConns is JETSTREAM_PG_MAX_CONNS's default (design §18).
const DefaultMaxConns = 16

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
	pcfg.MaxConns = cfg.MaxConns
	if pcfg.MaxConns <= 0 {
		pcfg.MaxConns = DefaultMaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pgstore: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgstore: connect: %w", err)
	}
	return &Store{pool: pool, connCfg: pcfg.ConnConfig.Copy(), metrics: cfg.Metrics}, nil
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
	reg.MustRegister(m.TxnDuration, m.TxnErrors, m.ListenReconnects)
	return m
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

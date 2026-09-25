package pgstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	listenChannel    = "jetstream_catalog"
	listenBackoffMin = 100 * time.Millisecond
	listenBackoffMax = 5 * time.Second
	listenBuffer     = 64
)

// Listen implements catalog.Listener over a dedicated connection outside
// the pool, so a follower's LISTEN never holds a pool slot. The first
// LISTEN must succeed; after that a lost connection is re-established with
// backoff until ctx ends. Notifications sent while it is down are lost,
// and a notification to a slow reader is dropped: the follower polls
// anyway (design §11.1).
func (s *Store) Listen(ctx context.Context) (<-chan uint64, error) {
	conn, err := s.listenConn(ctx)
	if err != nil {
		return nil, err
	}
	ch := make(chan uint64, listenBuffer)
	go s.listenLoop(ctx, conn, ch)
	return ch, nil
}

func (s *Store) listenConn(ctx context.Context) (*pgx.Conn, error) {
	conn, err := pgx.ConnectConfig(ctx, s.connCfg.Copy())
	if err != nil {
		return nil, fmt.Errorf("pgstore: listen connect: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+listenChannel); err != nil {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("pgstore: listen: %w", err)
	}
	return conn, nil
}

func (s *Store) listenLoop(ctx context.Context, conn *pgx.Conn, ch chan<- uint64) {
	defer close(ch)
	backoff := listenBackoffMin
	for {
		for conn == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			var err error
			if conn, err = s.listenConn(ctx); err != nil {
				backoff = min(2*backoff, listenBackoffMax)
				continue
			}
			backoff = listenBackoffMin
			s.metrics.reconnected()
		}
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			if ctx.Err() != nil {
				return
			}
			continue
		}
		rev, err := strconv.ParseUint(n.Payload, 10, 64)
		if err != nil {
			// Only leader transactions notify on this channel, with a
			// revision; anything else is someone else's traffic.
			continue
		}
		select {
		case ch <- rev:
		default:
		}
	}
}

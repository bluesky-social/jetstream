package storagefake

import (
	"context"
	"errors"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/metastore"
)

// ErrKilled is returned by every call through a killed Client: the process
// that owned its connections is gone.
var ErrKilled = errors.New("storagefake: client process killed")

// Client is one process's connections to the DB. The layer 3 oracle gives
// each pod its own, so the scheduler can tell pods' calls apart and a
// killed pod stops reaching the catalog at once, as a SIGKILLed process's
// connections do. The DB's own methods act as an anonymous client that
// cannot be killed.
type Client struct {
	db    *DB
	actor string
	// observer exempts the client from the fault schedule (Observer).
	observer bool

	killCtx context.Context
	kill    context.CancelFunc
}

var _ catalog.DB = (*Client)(nil)
var _ catalog.Listener = (*Client)(nil)

// Client returns a new client whose calls carry actor as their scheduler
// label prefix.
func (db *DB) Client(actor string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{db: db, actor: actor, killCtx: ctx, kill: cancel}
}

// Observer returns a client for a test harness's own reads. It is a
// Client, scheduler yields included, but the fault schedule never fires on
// it or counts its calls, so the harness cannot take a fault meant for a
// pod.
func (db *DB) Observer(actor string) *Client {
	c := db.Client(actor)
	c.observer = true
	return c
}

// Kill ends the client's process. Every later call fails with ErrKilled, a
// transaction in flight rolls back at its next call (COMMIT included), and
// its LISTEN channels close. A statement already past its scheduler point
// completes: the server had it before the connection died.
func (c *Client) Kill() { c.kill() }

// Killed reports whether Kill was called.
func (c *Client) Killed() bool { return c.killCtx.Err() != nil }

// exempt reports whether the fault schedule skips c's calls.
func (c *Client) exempt() bool { return c != nil && c.observer }

func (c *Client) alive() error {
	if c != nil && c.Killed() {
		return ErrKilled
	}
	return nil
}

func (c *Client) label(inner string) string {
	if inner == "" {
		return c.actor
	}
	return c.actor + "/" + inner
}

// Begin implements catalog.DB.
func (c *Client) Begin(ctx context.Context, kind catalog.TxKind) (catalog.Tx, error) {
	return c.db.begin(ctx, c, kind)
}

// BeginRead implements catalog.DB.
func (c *Client) BeginRead(ctx context.Context) (catalog.ReadTx, error) {
	return c.db.beginRead(ctx, c)
}

// ReadChanges implements catalog.DB.
func (c *Client) ReadChanges(ctx context.Context, q catalog.ChangesQuery) (catalog.Changes, error) {
	return c.db.readChanges(ctx, c, q)
}

// Listen implements catalog.Listener. The channel closes when ctx ends or
// the client is killed.
func (c *Client) Listen(ctx context.Context) (<-chan uint64, error) {
	if err := c.alive(); err != nil {
		return nil, err
	}
	lctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.killCtx, cancel)
	context.AfterFunc(lctx, func() { stop() })
	return c.db.Listen(lctx)
}

// NewLease returns a lease whose statements run on this client.
func (c *Client) NewLease() *Lease { return c.db.newLease(c) }

// MetaStore is DB.MetaStore on this client.
func (c *Client) MetaStore(commit func(ctx context.Context, ops []metastore.Op) error) metastore.Store {
	return &metaStore{db: c.db, cl: c, commit: commit}
}

// ExpireLease ends the current lease at now() without changing its holder
// or epoch, as if the holder's renewals had stopped reaching the database
// for a full lease. The holder's next Renew fails and any Acquire succeeds.
// It is an administrative seam: it takes no scheduler turn.
func (db *DB) ExpireLease(ctx context.Context) error {
	if err := db.lockArchive(ctx); err != nil {
		return err
	}
	defer db.unlockArchive()
	next := db.current().child()
	next.archive.LeaseExpiresAt = db.now()
	db.publish(next, nil, false)
	return nil
}

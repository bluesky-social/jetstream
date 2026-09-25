package storagefake

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
)

// FaultKind is what an injected fault does.
type FaultKind string

const (
	// FaultCommitFails fails COMMIT with nothing applied.
	FaultCommitFails FaultKind = "commit_fails"
	// FaultCommitLost applies the commit and then reports an error: the
	// connection died after COMMIT reached the server ("commit applied,
	// result unknown").
	FaultCommitLost FaultKind = "commit_lost"
	// FaultConnLost kills the connection at a statement: that statement
	// fails, the transaction aborts, and its locks are released at once.
	FaultConnLost FaultKind = "conn_lost"
	// FaultNotifyLost commits normally but delivers none of the
	// transaction's notifications.
	FaultNotifyLost FaultKind = "notify_lost"
	// FaultSlowRead delays BeginRead by Delay before taking the snapshot.
	FaultSlowRead FaultKind = "slow_read"
)

// Fault schedules one injected failure: Kind applies to the Ordinal-th
// (1-based) matching event. Commit faults count commits, FaultConnLost
// counts transactions begun, and FaultSlowRead counts BeginRead calls.
// Every fault sees every event, so ordinals count independently, and a
// scenario that faults exactly one step can observe the system across it.
//
// A Fault records whether it fired; Unfired lists the ones that did not, so
// a test proves its schedule actually happened (the oracle's "schedule,
// fired counter, Unfired check" pattern).
type Fault struct {
	Kind FaultKind
	// TxKind restricts transaction faults to one kind. Empty matches any.
	TxKind  catalog.TxKind
	Ordinal int
	// Statement is the 1-based statement FaultConnLost fires at. Zero
	// fires at COMMIT, before it reaches the server.
	Statement int
	// Delay is FaultSlowRead's delay.
	Delay time.Duration

	seen  atomic.Int64
	fired atomic.Int64
}

// Fired reports whether the fault fired.
func (f *Fault) Fired() bool { return f.fired.Load() > 0 }

func (f *Fault) String() string {
	s := fmt.Sprintf("%s #%d", f.Kind, f.Ordinal)
	if f.TxKind != "" {
		s += " of " + string(f.TxKind)
	}
	if f.Kind == FaultConnLost {
		s += fmt.Sprintf(" at statement %d", f.Statement)
	}
	return s
}

// InjectFaults adds faults to the schedule.
func (db *DB) InjectFaults(fs ...*Fault) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.faults = append(db.faults, fs...)
}

// Unfired returns the scheduled faults that have not fired.
func (db *DB) Unfired() []*Fault {
	db.mu.Lock()
	defer db.mu.Unlock()
	var out []*Fault
	for _, f := range db.faults {
		if !f.Fired() {
			out = append(out, f)
		}
	}
	return out
}

// match counts one event against every fault of the given kinds and
// returns the first whose ordinal it is.
func (db *DB) match(txKind catalog.TxKind, kinds ...FaultKind) *Fault {
	db.mu.Lock()
	fs := db.faults
	db.mu.Unlock()
	var hit *Fault
	for _, f := range fs {
		ok := false
		for _, k := range kinds {
			ok = ok || f.Kind == k
		}
		if !ok || (f.TxKind != "" && f.TxKind != txKind) {
			continue
		}
		if int(f.seen.Add(1)) == f.Ordinal && hit == nil {
			hit = f
		}
	}
	return hit
}

// armConnLost returns the connection-loss fault a new transaction carries,
// if any. It fires later, when the transaction reaches the statement.
func (db *DB) armConnLost(kind catalog.TxKind) *Fault {
	return db.match(kind, FaultConnLost)
}

func (db *DB) slowRead() time.Duration {
	f := db.match("", FaultSlowRead)
	if f == nil {
		return 0
	}
	f.fired.Add(1)
	return f.Delay
}

func (db *DB) commitFault(kind catalog.TxKind) *Fault {
	f := db.match(kind, FaultCommitFails, FaultCommitLost, FaultNotifyLost)
	if f != nil {
		f.fired.Add(1)
	}
	return f
}

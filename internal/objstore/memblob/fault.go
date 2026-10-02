package memblob

import (
	"strings"
	"sync/atomic"
)

// Op names a Blob operation that a FaultInjector can target.
type Op string

const (
	OpPut      Op = "put"
	OpGet      Op = "get"
	OpGetRange Op = "get_range"
	OpDelete   Op = "delete"
)

// FaultKind is what an injected fault does to one Blob operation.
type FaultKind string

const (
	// FaultNone lets the operation run normally.
	FaultNone FaultKind = ""

	// FaultError fails the operation with no effect. Applies to every op.
	FaultError FaultKind = "error"

	// FaultErrorAfter performs the operation and then returns an error: the
	// unknown-result case where a PUT or DELETE landed but its response was
	// lost. Applies to put and delete.
	FaultErrorAfter FaultKind = "error_after"

	// FaultWrongBytes flips a bit. On get and get_range it corrupts only the
	// returned copy (a bad read); on put it corrupts the stored object (a bad
	// write that every later read sees). Blob reports no error either way:
	// detection belongs to the Store layer.
	FaultWrongBytes FaultKind = "wrong_bytes"

	// FaultDropPut acknowledges a put without storing anything. Applies to
	// put only.
	FaultDropPut FaultKind = "drop_put"
)

func (k FaultKind) appliesTo(op Op) bool {
	switch k {
	case FaultError:
		return true
	case FaultErrorAfter:
		return op == OpPut || op == OpDelete
	case FaultWrongBytes:
		return op == OpPut || op == OpGet || op == OpGetRange
	case FaultDropPut:
		return op == OpPut
	}
	return false
}

// FaultInjector deterministically faults Blob operations in tests.
// Production code never installs one.
//
// BeforeBlobOp runs before each operation. It returns FaultNone to let the
// operation run, or a fault kind and, for FaultError and FaultErrorAfter,
// the error to return (nil gets a descriptive default). A kind that does not
// apply to op fails the operation. Implementations must be concurrency-safe.
type FaultInjector interface {
	BeforeBlobOp(op Op, key string) (FaultKind, error)
}

// KeyPrefixFault is the canonical FaultInjector: it applies Kind to the
// Ordinal-th (1-based) operation on a key under Prefix. Earlier and later
// matching operations run normally, so a scenario faults exactly one
// targeted operation and observes the system across that boundary.
//
// Matching is deterministic (key prefix plus occurrence count), so a failing
// run reproduces exactly.
type KeyPrefixFault struct {
	Prefix string
	// Op, when non-empty, restricts the fault to that operation. Empty
	// matches any operation.
	Op      Op
	Ordinal int
	// Kind defaults to FaultError when empty.
	Kind FaultKind
	Err  error

	seen atomic.Int64
}

// BeforeBlobOp implements FaultInjector. The occurrence counter is atomic,
// so the Ordinal-th matching operation fires exactly once even under
// concurrent callers.
func (f *KeyPrefixFault) BeforeBlobOp(op Op, key string) (FaultKind, error) {
	if f.Op != "" && f.Op != op {
		return FaultNone, nil
	}
	if !strings.HasPrefix(key, f.Prefix) {
		return FaultNone, nil
	}
	if int(f.seen.Add(1)) != f.Ordinal {
		return FaultNone, nil
	}
	if f.Kind == FaultNone {
		return FaultError, f.Err
	}
	return f.Kind, f.Err
}

// Faults applies several injectors in order; the first to return a fault
// wins. Every injector sees every operation, so each one's ordinal counts
// independently of the others.
type Faults []FaultInjector

// BeforeBlobOp implements FaultInjector.
func (fs Faults) BeforeBlobOp(op Op, key string) (FaultKind, error) {
	kind, err := FaultNone, error(nil)
	for _, f := range fs {
		k, e := f.BeforeBlobOp(op, key)
		if kind == FaultNone && k != FaultNone {
			kind, err = k, e
		}
	}
	return kind, err
}

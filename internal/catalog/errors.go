package catalog

import (
	"errors"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/leader"
)

// ErrSessionEnded marks a leader transaction that failed, or whose commit
// result is unknown (design §9.1). It wraps leader.ErrRestartSession, so the
// election loop starts a fresh session that rebuilds from what actually
// committed. A Session that returned it refuses every later transaction:
// nothing is ever retried inside the session that saw the failure.
var ErrSessionEnded = fmt.Errorf("catalog: leader session ended: %w", leader.ErrRestartSession)

// ErrFenced means the fence statement matched no row: another pod holds a
// newer writer epoch (design §6.4). It is a session-ending error.
var ErrFenced = fmt.Errorf("catalog: fenced out by a newer writer epoch: %w", ErrSessionEnded)

// Corruption sources for jetstream_storage_corruption_total{source}.
const (
	SourceSeq        = "seq"        // stored seq key disagrees with the commit
	SourceRef        = "ref"        // a referenced object is not available
	SourceFold       = "fold"       // fold coverage is not exact
	SourceSeal       = "seal"       // active blocks differ from the footer's list
	SourceInvariant  = "invariant"  // a §9.3 catalog invariant is broken
	SourceObject     = "object"     // object row state is impossible
	SourceRead       = "read"       // a referenced object is missing or fails its hash
	SourceMeta       = "meta"       // a catalog-owned metadata value is malformed
	SourceHotBatch   = "hot_batch"  // a hot batch row or frame is malformed
	SourceGeneration = "generation" // a generation header or footer is malformed
)

// CorruptionError is storage corruption or a broken catalog invariant (design
// §6.5). Unlike ErrSessionEnded it is fatal: the election loop exits the
// process, because every leader would hit the same state.
type CorruptionError struct {
	Source string
	Err    error
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("catalog: storage corruption (%s): %v", e.Source, e.Err)
}

func (e *CorruptionError) Unwrap() error { return e.Err }

// SessionFatal makes leader.DefaultFatal treat the error as fatal even when
// a caller has also wrapped it with a restart marker.
func (e *CorruptionError) SessionFatal() bool { return true }

// Corruptf returns a CorruptionError for source.
func Corruptf(source, format string, args ...any) error {
	return &CorruptionError{Source: source, Err: fmt.Errorf(format, args...)}
}

// IsCorruption reports whether err is, or wraps, a CorruptionError, and
// returns its source.
func IsCorruption(err error) (string, bool) {
	var ce *CorruptionError
	if errors.As(err, &ce) {
		return ce.Source, true
	}
	return "", false
}

// sessionEnded wraps a transaction failure as session-ending. Corruption
// passes through unchanged so it stays fatal.
func sessionEnded(op string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := IsCorruption(err); ok {
		return err
	}
	if errors.Is(err, ErrSessionEnded) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", ErrSessionEnded, op, err)
}

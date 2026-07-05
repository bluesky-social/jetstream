package world

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/gt"
)

// identityVariantMix splits the random-mix #identity draws between the
// two payload shapes. A 180s production firehose sample (2026-07-04,
// 64k events) contained 39 #identity events, every one of them
// handle-absent — so handle-absent is the dominant real-world shape and
// gets the majority weight, while handle-change keeps the
// optional-field payload path exercised.
var identityVariantMix = []weighted[string]{
	{value: "absent", weight: 70},
	{value: "handle", weight: 30},
}

// generateIdentity emits one #identity frame for account idx, drawing
// the payload variant (handle-absent vs handle-change) from the world
// RNG. Callers must hold mutationMu.
func (w *World) generateIdentity(ctx context.Context, idx int) ([]byte, error) {
	if weightedChoice(w.rng, identityVariantMix) == "handle" {
		return w.generateIdentityHandleChange(ctx, idx)
	}
	return w.generateIdentityAbsent(ctx, idx)
}

// generateIdentityAbsent emits an #identity frame with no handle field
// — the shape a PLC operation that doesn't change the handle produces,
// and the dominant shape on the production firehose.
func (w *World) generateIdentityAbsent(ctx context.Context, idx int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a, err := w.loadAccount(idx)
	if err != nil {
		return nil, err
	}
	return w.emitIdentityFrame(string(a.DID), gt.None[string](), nil)
}

// generateIdentityHandleChange bumps the account's persisted
// handle-change counter and emits an #identity frame carrying the new
// handle. The counter makes repeated changes produce genuinely
// distinct handles while staying deterministic across runs. The
// counter write is staged in the same batch as the frame so a crash
// cannot separate them.
func (w *World) generateIdentityHandleChange(ctx context.Context, idx int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a, err := w.loadAccount(idx)
	if err != nil {
		return nil, err
	}
	n, err := w.loadHandleChangeCount(idx)
	if err != nil {
		return nil, err
	}
	n++
	handle := fmt.Sprintf("user-%d-h%d.test", idx, n)
	return w.emitIdentityFrame(string(a.DID), gt.Some(handle), func(b *pebble.Batch) error {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], n)
		if err := b.Set(keyAccountHandleChanges(idx), buf[:], nil); err != nil {
			return fmt.Errorf("world: stage handle-change count %d: %w", idx, err)
		}
		return nil
	})
}

// generateMalformedIdentity emits an #identity frame whose DID fails
// atproto DID syntax ('!' is outside the identifier charset). Upstream
// relays do not signature-verify #identity bodies, so a malformed DID
// reaches consumers as-is; jetstream archives it byte-faithfully and
// the net-new backfill enqueuer is the validation boundary that must
// reject it. Never part of the random mix — injection-only, so the
// polite-by-default world contract holds.
func (w *World) generateMalformedIdentity(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return w.emitIdentityFrame(MalformedIdentityDID, gt.None[string](), nil)
}

// MalformedIdentityDID is the syntactically-invalid DID carried by
// GenerateMalformedIdentityForTest frames. Exported so oracle asserts
// can locate the archived row and the enqueuer rejection it must cause.
const MalformedIdentityDID = "did:plc:oracle!malformed"

// emitIdentityFrame is the shared tail: allocate a seq, stamp the
// logical clock, encode, persist to firehose history, and publish.
// stageExtra, when non-nil, adds caller writes to the same batch.
// Mirrors generateAccountDelete's frame recipe.
func (w *World) emitIdentityFrame(did string, handle gt.Option[string], stageExtra func(*pebble.Batch) error) ([]byte, error) {
	seq := w.seq.Add(1)
	b := w.db.NewBatch()
	defer func() { _ = b.Close() }()
	eventMicros, err := w.nextLogicalClockMicros(b)
	if err != nil {
		return nil, err
	}
	envelope := &comatproto.SyncSubscribeRepos_Identity{
		DID:    did,
		Handle: handle,
		Seq:    seq,
		Time:   formatLogicalClockTime(eventMicros),
	}
	frame, err := encodeIdentityFrame(envelope)
	if err != nil {
		return nil, err
	}
	if stageExtra != nil {
		if err := stageExtra(b); err != nil {
			return nil, err
		}
	}
	if err := stageFirehoseFrame(b, seq, frame, w.cfg.FirehoseHistory); err != nil {
		return nil, err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return nil, fmt.Errorf("world: commit identity frame: %w", err)
	}
	if w.fanout != nil {
		w.fanout.Publish(frame)
	}
	return frame, nil
}

// loadHandleChangeCount reads the per-account handle-change counter;
// absence means zero.
func (w *World) loadHandleChangeCount(idx int) (uint64, error) {
	val, closer, err := w.db.Get(keyAccountHandleChanges(idx))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("world: load handle-change count %d: %w", idx, err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, fmt.Errorf("world: handle-change count %d has %d bytes, want 8", idx, len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

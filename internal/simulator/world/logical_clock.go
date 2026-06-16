package world

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
)

const logicalClockKey = "sim/logical_clock"
const logicalClockBaseMicros int64 = 1_700_000_000_000_000
const logicalClockStepMicros int64 = 1
const logicalClockID uint = 0

func (w *World) nextRev(b *pebble.Batch) (string, error) {
	next, err := w.nextLogicalClockMicros(b)
	if err != nil {
		return "", err
	}
	return string(atmos.NewTID(next, logicalClockID)), nil
}

func (w *World) nextLogicalClockMicros(b *pebble.Batch) (int64, error) {
	cur, err := w.loadLogicalClock()
	if err != nil {
		return 0, err
	}
	if cur == 0 {
		cur = logicalClockBaseMicros
	}

	next := cur + logicalClockStepMicros
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(next))
	if err := b.Set([]byte(logicalClockKey), buf[:], nil); err != nil {
		return 0, fmt.Errorf("world: stage logical clock: %w", err)
	}
	return next, nil
}

func formatLogicalClockTime(micros int64) string {
	return time.UnixMicro(micros).UTC().Format("2006-01-02T15:04:05.000Z")
}

func (w *World) loadLogicalClock() (int64, error) {
	val, closer, err := w.db.Get([]byte(logicalClockKey))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("world: load logical clock: %w", err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) != 8 {
		return 0, fmt.Errorf("world: logical clock has %d bytes, want 8", len(val))
	}
	return int64(binary.BigEndian.Uint64(val)), nil
}

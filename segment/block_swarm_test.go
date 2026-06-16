package segment

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Swarm test (Groce et al.): each iteration independently flips
// each axis with p=0.5, biasing the generator toward the enabled
// features. Iterations therefore cover the full subset lattice,
// including sparse "one feature alone" cases that uniform random
// would almost never hit.
//
// If zero axes end up enabled the iteration is just a default-
// uniform draw, which the property test in block_test.go already
// covers, so we force at least one axis on.
//
// The test is skipped under -short so `just test-short` (and the
// default `just` recipe that uses it) stays under one second.
// `just test` and `just test-race` run it; bounds below are sized
// so even the race-detector run completes in a few seconds.

const (
	axisTinyPayloads = iota + 1
	axisModeratePayloads
	axisEmptyOptionals
	axisMaxLengthDIDs
	axisSizeExtreme
	axisSameKind
	axisRepeatedEvents
	axisMostlyZeroColumns
	axisLengthPrefixBytes
	numAxes
)

type swarmFlags [numAxes]bool

func (f swarmFlags) any() bool {
	for _, b := range f {
		if b {
			return true
		}
	}
	return false
}

func TestSwarm(t *testing.T) {
	t.Parallel()

	iterations := 50
	if !testing.Short() {
		iterations = 1000
	}

	for iter := 0; iter < iterations; iter++ {
		// Each iteration uses its own deterministic seed so a failure
		// is reproducible by re-running with the printed seed.
		seed := int64(iter)
		r := rand.New(rand.NewSource(seed))

		var flags swarmFlags
		for i := range flags {
			flags[i] = r.Intn(2) == 0
		}
		if !flags.any() {
			flags[r.Intn(numAxes)] = true
		}

		events := generateSwarmBlock(r, flags)
		if len(events) == 0 {
			continue
		}

		encoded, err := encodeBlockCompressed(events)
		require.NoErrorf(t, err, "iter=%d seed=%d flags=%v encode", iter, seed, flags)

		decoded, err := decodeBlockCompressed(encoded)
		require.NoErrorf(t, err, "iter=%d seed=%d flags=%v decode", iter, seed, flags)

		require.Equalf(t, len(events), len(decoded),
			"iter=%d seed=%d flags=%v size mismatch", iter, seed, flags)
		for i := range events {
			require.Truef(t, eventsEqual(events[i], decoded[i]),
				"iter=%d seed=%d flags=%v event %d mismatch", iter, seed, flags, i)
		}
	}
}

func generateSwarmBlock(r *rand.Rand, f swarmFlags) []Event {
	// Block size axis. The "extreme" arm caps at 1024 (not 4096) to
	// keep wall-clock manageable; combined with axisMaxLengthDIDs and
	// axisRepeatedEvents it would otherwise produce ~64MB pre-zstd.
	var n int
	switch {
	case f[axisSizeExtreme] && r.Intn(2) == 0:
		n = 1
	case f[axisSizeExtreme]:
		n = 1024
	default:
		n = 1 + r.Intn(32)
	}

	// Kind axis.
	pickKind := func() Kind { return Kind(1 + r.Intn(7)) }
	if f[axisSameKind] {
		k := pickKind()
		pickKind = func() Kind { return k }
	}

	// "Repeated events" axis: build one and clone it.
	if f[axisRepeatedEvents] {
		template := buildSwarmEvent(r, f, pickKind)
		out := make([]Event, n)
		for i := range out {
			out[i] = template
		}
		return out
	}

	out := make([]Event, n)
	for i := range out {
		out[i] = buildSwarmEvent(r, f, pickKind)
	}
	return out
}

func buildSwarmEvent(r *rand.Rand, f swarmFlags, pickKind func() Kind) Event {
	ev := Event{Kind: pickKind()}

	// DID.
	if f[axisMaxLengthDIDs] {
		ev.DID = strings.Repeat("d", 65535)
	} else {
		ev.DID = randString(r, 16+r.Intn(48))
	}

	// Empty-optionals axis: leave Collection/Rkey/Rev/Payload at zero values.
	if !f[axisEmptyOptionals] {
		ev.Collection = randString(r, 1+r.Intn(64))
		ev.Rkey = randString(r, 1+r.Intn(20))
		ev.Rev = randString(r, 1+r.Intn(20))
	}

	// Payload size axis. Bounds are deliberately small to keep the
	// race-detector run fast. The property test in block_test.go
	// covers larger payloads with realistic distributions; swarm's
	// value is feature-combination coverage, not raw payload bytes.
	switch {
	case f[axisTinyPayloads]:
		ev.Payload = randBytes(r, r.Intn(11)) // 0..10
	case f[axisModeratePayloads]:
		ev.Payload = randBytes(r, 4*1024+r.Intn(12*1024)) // 4..16 KB
	default:
		if !f[axisEmptyOptionals] {
			ev.Payload = randBytes(r, r.Intn(2048))
		}
	}

	// Mostly-zero columns axis.
	if f[axisMostlyZeroColumns] {
		ev.Seq = 0
		ev.IndexedAt = 0
		ev.RenderedAt = 0
	} else {
		ev.Seq = r.Uint64()
		ev.IndexedAt = int64(r.Uint64())
		ev.RenderedAt = int64(r.Uint64())
	}

	// Length-prefix-bytes axis: stuff payload with bytes that look
	// like length headers, to confuse a buggy decoder.
	if f[axisLengthPrefixBytes] && len(ev.Payload) >= 8 {
		// Place bytes that look like a uint32 = 1<<31 at the start.
		for i := 0; i < 4 && i < len(ev.Payload); i++ {
			ev.Payload[i] = 0xFF
		}
	}

	return ev
}

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

const (
	axisTinyPayloads = iota
	axisHugePayloads
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

// swarmConfig parameterizes how aggressive an iteration is. The
// always-on TestSwarm uses the modest config; TestSwarmLong uses the
// heavy one and skips under -short.
type swarmConfig struct {
	iterations     int
	maxBlockN      int // upper bound for the typical-size axis
	hugePayload    int // upper bound for axisHugePayloads
	hugePayloadMin int // lower bound for axisHugePayloads
}

// The unique value of swarm is feature-combination coverage, not raw
// iteration count: ~50 iterations × 9 axes × p=0.5 already samples
// the subset lattice well. We deliberately do not crank iterations to
// thousands; the property test (block_test.go) covers raw randomness.
//
// Cost in this test is dominated by zstd compression of large
// payloads and the axisMaxLengthDIDs × axisRepeatedEvents combination
// (which produces ~4MB of pre-compression bytes per block). Tighten
// hugePayload before iterations if wall-clock grows.
var (
	swarmConfigShort = swarmConfig{
		iterations:     50,
		maxBlockN:      32,
		hugePayloadMin: 8 * 1024,
		hugePayload:    32 * 1024,
	}
	swarmConfigLong = swarmConfig{
		iterations:     250,
		maxBlockN:      64,
		hugePayloadMin: 64 * 1024,
		hugePayload:    256 * 1024,
	}
)

func TestSwarm(t *testing.T) {
	t.Parallel()
	runSwarm(t, swarmConfigShort)
}

func TestSwarmLong(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping long swarm test under -short")
	}
	runSwarm(t, swarmConfigLong)
}

func runSwarm(t *testing.T, cfg swarmConfig) {
	t.Helper()
	for iter := 0; iter < cfg.iterations; iter++ {
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

		events := generateSwarmBlock(r, flags, cfg)
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

func generateSwarmBlock(r *rand.Rand, f swarmFlags, cfg swarmConfig) []Event {
	// Block size axis.
	var n int
	switch {
	case f[axisSizeExtreme] && r.Intn(2) == 0:
		n = 1
	case f[axisSizeExtreme]:
		n = 4096
	default:
		n = 1 + r.Intn(cfg.maxBlockN)
	}

	// Kind axis.
	pickKind := func() Kind { return Kind(1 + r.Intn(6)) }
	if f[axisSameKind] {
		k := pickKind()
		pickKind = func() Kind { return k }
	}

	// "Repeated events" axis: build one and clone it.
	if f[axisRepeatedEvents] {
		template := buildSwarmEvent(r, f, pickKind, cfg)
		out := make([]Event, n)
		for i := range out {
			out[i] = template
		}
		return out
	}

	out := make([]Event, n)
	for i := range out {
		out[i] = buildSwarmEvent(r, f, pickKind, cfg)
	}
	return out
}

func buildSwarmEvent(r *rand.Rand, f swarmFlags, pickKind func() Kind, cfg swarmConfig) Event {
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

	// Payload size axis. Bounds come from cfg so the short and long
	// variants differ only in their payload aggressiveness.
	switch {
	case f[axisTinyPayloads]:
		ev.Payload = randBytes(r, r.Intn(11)) // 0..10
	case f[axisHugePayloads]:
		span := cfg.hugePayload - cfg.hugePayloadMin
		if span <= 0 {
			span = 1
		}
		ev.Payload = randBytes(r, cfg.hugePayloadMin+r.Intn(span))
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

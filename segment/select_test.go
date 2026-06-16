package segment

import (
	"testing"

	"github.com/jcalabro/gloom"
	"github.com/stretchr/testify/require"
)

func bloomWith(dids ...string) *gloom.Filter {
	f := gloom.New(uint64(len(dids)+1), 0.001)
	for _, d := range dids {
		f.AddString(d)
	}
	return f
}

func TestSelectBlocksForDID(t *testing.T) {
	t.Parallel()

	const target = "did:plc:needle"

	t.Run("segment bloom miss selects nothing", func(t *testing.T) {
		t.Parallel()
		seg := bloomWith("did:plc:other")
		blocks := []*gloom.Filter{bloomWith("did:plc:other")}
		require.Empty(t, SelectBlocksForDID(seg, blocks, target))
	})

	t.Run("no false negative: block holding did is selected", func(t *testing.T) {
		t.Parallel()
		seg := bloomWith("did:plc:a", target, "did:plc:b")
		blocks := []*gloom.Filter{
			bloomWith("did:plc:a"),
			bloomWith(target),
			bloomWith("did:plc:b"),
		}
		got := SelectBlocksForDID(seg, blocks, target)
		require.Contains(t, got, 1)
	})

	t.Run("prunes blocks that do not hold did", func(t *testing.T) {
		t.Parallel()
		seg := bloomWith("did:plc:a", target)
		// Three decoy blocks, target only in block 2.
		blocks := []*gloom.Filter{
			bloomWith("did:plc:a"),
			bloomWith("did:plc:b"),
			bloomWith(target),
			bloomWith("did:plc:c"),
		}
		got := SelectBlocksForDID(seg, blocks, target)
		require.Contains(t, got, 2)
		require.Less(t, len(got), 4) // not everything
	})

	t.Run("nil segment bloom does not short-circuit", func(t *testing.T) {
		t.Parallel()
		blocks := []*gloom.Filter{bloomWith(target)}
		got := SelectBlocksForDID(nil, blocks, target)
		require.Contains(t, got, 0)
	})

	t.Run("nil per-block bloom is conservatively included", func(t *testing.T) {
		t.Parallel()
		// segment bloom must pass for us to reach per-block logic.
		seg := bloomWith(target)
		blocks := []*gloom.Filter{nil, bloomWith("did:plc:other")}
		got := SelectBlocksForDID(seg, blocks, target)
		require.Contains(t, got, 0) // nil bloom -> include rather than risk a miss
	})

	t.Run("empty blocks selects nothing", func(t *testing.T) {
		t.Parallel()
		require.Empty(t, SelectBlocksForDID(bloomWith(target), nil, target))
	})

	t.Run("returned indices are ascending", func(t *testing.T) {
		t.Parallel()
		seg := bloomWith(target)
		blocks := []*gloom.Filter{
			bloomWith(target), bloomWith("x"), bloomWith(target), bloomWith(target),
		}
		got := SelectBlocksForDID(seg, blocks, target)
		for i := 1; i < len(got); i++ {
			require.Less(t, got[i-1], got[i])
		}
	})
}

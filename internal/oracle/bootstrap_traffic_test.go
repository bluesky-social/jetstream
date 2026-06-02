package oracle

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapTrafficGeneratorDistributesTargetAcrossRepoCompletions(t *testing.T) {
	t.Parallel()

	var generated int
	gen := newBootstrapTrafficGenerator(4, 10, func(context.Context) (int64, error) {
		generated++
		return int64(generated), nil
	})

	for range 4 {
		require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:test"))
	}

	require.Equal(t, 10, generated)

	require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:extra"))
	require.Equal(t, 10, generated, "extra completion callbacks must not over-generate")
}

func TestBootstrapTrafficGeneratorNoopsWhenTargetIsZero(t *testing.T) {
	t.Parallel()

	var generated int
	gen := newBootstrapTrafficGenerator(4, 0, func(context.Context) (int64, error) {
		generated++
		return int64(generated), nil
	})

	require.NoError(t, gen.AfterRepoComplete(t.Context(), "did:plc:test"))
	require.Zero(t, generated)
}

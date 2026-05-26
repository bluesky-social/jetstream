package world

import (
	"strings"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/stretchr/testify/require"
)

func TestDeriveAccount_Deterministic(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 7)
	require.NoError(t, err)
	b, err := deriveAccount(42, 7)
	require.NoError(t, err)
	require.Equal(t, a.DID, b.DID)
	require.Equal(t, a.PrivKeyBytes, b.PrivKeyBytes)
}

func TestDeriveAccount_DifferentInputsDiffer(t *testing.T) {
	t.Parallel()
	a1, _ := deriveAccount(42, 1)
	a2, _ := deriveAccount(42, 2)
	a3, _ := deriveAccount(43, 1)
	require.NotEqual(t, a1.DID, a2.DID)
	require.NotEqual(t, a1.DID, a3.DID)
}

func TestDeriveAccount_DIDIsValid(t *testing.T) {
	t.Parallel()
	a, err := deriveAccount(42, 0)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(a.DID), "did:plc:"))
	require.Len(t, a.DID.Identifier(), 24)
	_, err = atmos.ParseDID(string(a.DID))
	require.NoError(t, err)
}

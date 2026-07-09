package zstddict

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseID(t *testing.T) {
	t.Parallel()
	d := make([]byte, 16)
	binary.LittleEndian.PutUint32(d[:4], 0xEC30A437)
	binary.LittleEndian.PutUint32(d[4:8], 20260709)
	id, err := ParseID(d)
	require.NoError(t, err)
	require.Equal(t, uint32(20260709), id)
}

func TestParseID_Rejects(t *testing.T) {
	t.Parallel()
	_, err := ParseID(nil)
	require.Error(t, err, "empty")
	_, err = ParseID([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	require.Error(t, err, "bad magic")
	d := make([]byte, 8)
	binary.LittleEndian.PutUint32(d[:4], 0xEC30A437)
	_, err = ParseID(d)
	require.Error(t, err, "zero ID")
}

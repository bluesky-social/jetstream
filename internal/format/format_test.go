package format

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInt(t *testing.T) {
	t.Parallel()
	require.Equal(t, "0", Int(0))
	require.Equal(t, "999", Int(999))
	require.Equal(t, "1,000", Int(1000))
	require.Equal(t, "1,234,567", Int(1234567))
}

func TestBytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{int64(1.5 * 1024 * 1024 * 1024), "1.50 GiB"},
		{int64(2 * 1024 * 1024 * 1024 * 1024), "2.00 TiB"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, Bytes(c.in), "Bytes(%d)", c.in)
	}
}

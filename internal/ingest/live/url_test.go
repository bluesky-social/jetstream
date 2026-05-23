package live

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeriveSubscribeReposURL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
		err  bool
	}{
		{
			name: "https",
			in:   "https://bsky.network",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "http",
			in:   "http://localhost:2470",
			want: "ws://localhost:2470/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "trailing slash stripped",
			in:   "https://bsky.network/",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "with path discarded",
			in:   "https://bsky.network/some/path",
			want: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos",
		},
		{
			name: "missing scheme is an error",
			in:   "bsky.network",
			err:  true,
		},
		{
			name: "unsupported scheme is an error",
			in:   "ftp://bsky.network",
			err:  true,
		},
		{
			name: "empty is an error",
			in:   "",
			err:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := deriveSubscribeReposURL(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDeriveRelayHTTPURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
		wantErr  bool
	}{
		{"https://bsky.network", "https://bsky.network", false},
		{"http://localhost:2470", "http://localhost:2470", false},
		{"https://bsky.network/xrpc/com.atproto.sync.subscribeRepos", "https://bsky.network", false},
		{"wss://bsky.network", "https://bsky.network", false},
		{"ws://localhost:2470", "http://localhost:2470", false},
		{"", "", true},
		{"://no-scheme", "", true},
		{"ftp://bad-scheme.example", "", true},
	}
	for _, tc := range cases {
		got, err := DeriveRelayHTTPURL(tc.in)
		if tc.wantErr {
			require.Error(t, err, "input=%q", tc.in)
			continue
		}
		require.NoError(t, err, "input=%q", tc.in)
		require.Equal(t, tc.want, got, "input=%q", tc.in)
	}
}

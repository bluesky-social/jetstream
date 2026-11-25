package repair

import (
	"fmt"
	"testing"

	"github.com/bluesky-social/jetstream/internal/objstore/memblob"
	"github.com/bluesky-social/jetstream/segment"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestStageStore_RoundTripResumeAndIntegrity(t *testing.T) {
	t.Parallel()

	backends := []struct {
		name  string
		build func(t *testing.T) StageStore
	}{
		{
			name:  "mem",
			build: func(*testing.T) StageStore { return NewMemStageStore() },
		},
		{
			name:  "fs_mem",
			build: func(*testing.T) StageStore { return NewFSStageStore(vfs.NewMem(), "/repair") },
		},
		{
			name:  "blob",
			build: func(*testing.T) StageStore { return NewBlobStageStore(memblob.New(), "archive-1/repair/") },
		},
		{
			name: "blob_multipart",
			build: func(*testing.T) StageStore {
				return NewBlobStageStoreWithPartSize(memblob.New(), "archive-1/repair/", 64)
			},
		},
	}

	events := make([]segment.Event, 15)
	events[0] = segment.Event{
		Kind:                segment.KindSync,
		UpstreamRelayCursor: 42,
		DID:                 "did:plc:stage0000000000000000000",
		Rev:                 "3lrev00000001",
		Payload:             []byte{0xa0},
	}
	for i := 1; i < len(events); i++ {
		events[i] = segment.Event{
			Kind:                segment.KindCreateResync,
			UpstreamRelayCursor: 42,
			DID:                 "did:plc:stage0000000000000000000",
			Collection:          "app.bsky.feed.post",
			Rkey:                fmt.Sprintf("rk%04d", i),
			Rev:                 "3lrev00000001",
			Payload:             []byte(fmt.Sprintf("payload-%d", i)),
		}
	}

	for _, tc := range backends {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := tc.build(t)
			ctx := t.Context()

			sw, err := NewStageWriter(ctx, store, "job-1.stage")
			require.NoError(t, err)
			for i := range events {
				require.NoError(t, sw.WriteEvent(&events[i]))
			}
			meta, err := sw.Commit(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(len(events)), meta.TotalRows)
			require.Greater(t, meta.SizeBytes, int64(0))

			// Verify full read and mid-stream resume at every boundary.
			for _, skip := range []int64{0, 1, 7, int64(len(events) - 1), int64(len(events))} {
				sr, err := OpenStageReader(ctx, store, meta.Key, meta.SHA256, skip)
				require.NoError(t, err, "skip=%d", skip)
				got := make([]segment.Event, 0)
				for {
					var ev segment.Event
					ok, err := sr.Next(&ev)
					require.NoError(t, err)
					if !ok {
						break
					}
					got = append(got, ev)
				}
				require.NoError(t, sr.Close())
				require.Equal(t, events[skip:], got, "skip=%d", skip)
			}

			// Tampered checksum must fail loud before yielding any row.
			badSum := meta.SHA256
			badSum[0] ^= 0xff
			_, err = OpenStageReader(ctx, store, meta.Key, badSum, 0)
			require.ErrorContains(t, err, "checksum mismatch")

			// Abort leaves no committed artifact.
			swAbort, err := NewStageWriter(ctx, store, "job-aborted.stage")
			require.NoError(t, err)
			require.NoError(t, swAbort.WriteEvent(&events[0]))
			require.NoError(t, swAbort.Abort())
			_, err = store.Open(ctx, "job-aborted.stage")
			require.Error(t, err)

			// Delete removes committed artifact.
			require.NoError(t, store.Delete(ctx, meta.Key))
			_, err = store.Open(ctx, meta.Key)
			require.Error(t, err)
		})
	}
}

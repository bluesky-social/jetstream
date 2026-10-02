package pebblestore_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/bluesky-social/jetstream/internal/metastore"
	"github.com/bluesky-social/jetstream/internal/metastore/memstore"
	"github.com/bluesky-social/jetstream/internal/metastore/storetest"
	"github.com/stretchr/testify/require"
)

// FuzzBatchSemanticsAgree drives Pebble and memstore with the same random op
// sequence and requires identical contents after every commit. memstore is
// the reference model for the PG impl and storagefake, so any divergence
// here is a divergence between local and disaggregated metadata.
//
// Each op is decoded from 3 bytes: kind, key, and a second key (DeleteRange
// end) or value byte. Keys come from a small alphabet that includes 0x00 and
// 0xff so prefix and ordering edges get exercised.
func FuzzBatchSemanticsAgree(f *testing.F) {
	f.Add([]byte{0, 1, 2, 1, 1, 0, 0, 1, 3, 6, 0, 0})
	f.Add([]byte{0, 3, 1, 0, 4, 2, 2, 0, 7, 0, 5, 9, 6, 0, 0})
	f.Add([]byte{3, 0, 7, 0, 7, 1, 6, 0, 0, 2, 7, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		ctx := context.Background()
		p := openMem(t)
		m := memstore.New()
		stores := []metastore.Store{p, m}
		batches := []metastore.Batch{p.NewBatch(), m.NewBatch()}

		check := func() {
			want := storetest.Scan(t, m, nil, nil)
			require.Equal(t, want, storetest.Scan(t, p, nil, nil))
			for _, k := range fuzzKeys {
				lo, hi := []byte(k), metastore.PrefixUpperBound([]byte(k))
				require.Equal(t, storetest.Scan(t, m, lo, hi), storetest.Scan(t, p, lo, hi), "prefix %q", k)
			}
		}

		for len(ops) >= 3 {
			kind, a, c := ops[0]%7, fuzzKey(ops[1]), ops[2]
			ops = ops[3:]
			switch kind {
			case 0:
				for _, b := range batches {
					b.Set(a, []byte{c})
				}
			case 1:
				for _, b := range batches {
					b.Delete(a)
				}
			case 2:
				start, end := a, fuzzKey(c)
				switch bytes.Compare(start, end) {
				case 0:
					continue
				case 1:
					start, end = end, start
				}
				for _, b := range batches {
					b.DeleteRange(start, end)
				}
			case 3:
				require.Equal(t, batches[1].Len(), batches[0].Len())
				for i := range batches {
					require.NoError(t, batches[i].Commit(ctx))
					batches[i] = stores[i].NewBatch()
				}
				check()
			case 4:
				for _, s := range stores {
					require.NoError(t, s.Set(ctx, a, []byte{c}))
				}
			case 5:
				for _, s := range stores {
					require.NoError(t, s.Delete(ctx, a))
				}
			case 6:
				pv, perr := p.Get(ctx, a)
				mv, merr := m.Get(ctx, a)
				require.Equal(t, merr, perr)
				require.Equal(t, mv, pv)
			}
		}
		for i := range batches {
			require.NoError(t, batches[i].Commit(ctx))
		}
		check()
	})
}

var fuzzKeys = []string{"", "\x00", "a", "a\x00", "a\xff", "ab", "b", "b\x00b", "\xff", "\xff\x00", "\xff\xff"}

func fuzzKey(b byte) []byte { return []byte(fuzzKeys[int(b)%len(fuzzKeys)]) }

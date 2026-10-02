package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/ingest/backfill"
	"github.com/bluesky-social/jetstream/internal/metastore"
	metapg "github.com/bluesky-social/jetstream/internal/metastore/pg"
	"github.com/bluesky-social/jetstream/internal/pgstore/pgfixture"
	"github.com/jcalabro/atmos"
	"github.com/urfave/cli/v3"
)

func retryScanCommand() *cli.Command {
	return &cli.Command{
		Name:  "retryscan",
		Usage: "Load metadata_kv with repo/ rows, then time the failed-repo retry pass's full scan",
		Description: `The retry pass (backfill.retryRunner.scanDue) reads and decodes
every repo/ row each DefaultFailedRepoRetryInterval to find the due failed
ones. pop1 has about 40M rows. This loads --repos rows shaped as a steady-state
store holds them (complete rows, with --failed-share failed ones), then times
full scans through the leader's metastore: a raw iteration, and the same
iteration decoding every row as scanDue does (backfill.CountStatuses).

Run it at two sizes to check that the cost is linear before extrapolating to
--pop1-repos. The scans run right after the load, so the rows are in
PostgreSQL's and the kernel's caches: a pop1 table is much larger than
either, so treat the time as a floor and the bytes as the measure.`,
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "repos", Value: 1_000_000, Validator: atLeastOne[int]},
			&cli.Float64Flag{Name: "failed-share", Value: 0.02},
			&cli.IntFlag{Name: "batch", Value: 5000, Usage: "Rows per load transaction", Validator: atLeastOne[int]},
			&cli.IntFlag{Name: "passes", Value: 2, Usage: "Timed passes of each scan", Validator: atLeastOne[int]},
			&cli.Uint64Flag{Name: "seed", Value: 1},
			&cli.Uint64Flag{Name: "pop1-repos", Value: 40_000_000},
		},
		Action: runRetryScan,
	}
}

// repoRow is one synthetic repo/<did> value. Complete rows carry what a
// completion stages (backfill.Store.stageDurableBatch); failed rows carry a
// retry schedule and an error.
func repoRow(rng *rand.Rand, now time.Time, failedShare float64) backfill.RepoStatus {
	host := fmt.Sprintf("pds%d.us-east.host.bsky.network", 1+rng.IntN(100))
	at := now.Add(-time.Duration(rng.Int64N(int64(90 * 24 * time.Hour))))
	rev := string(atmos.NewTIDFromTime(at, uint(rng.IntN(1024))))
	rs := backfill.RepoStatus{PDS: "https://" + host, Host: host, Active: rng.IntN(50) != 0}
	if rng.Float64() < failedShare {
		rs.Backfill = backfill.RepoBackfillStatus{
			Status:        backfill.StatusFailed,
			Attempts:      1 + rng.IntN(5),
			RetryCount:    rng.IntN(20),
			LastError:     "backfill: get repo: xrpc: 502 Bad Gateway: upstream connect error or disconnect/reset before headers",
			NextAttemptAt: now.Add(time.Duration(rng.Int64N(int64(24 * time.Hour)))),
			StartedAt:     at,
		}
		rs.LastAttemptedAt = at
		return rs
	}
	rs.Backfill = backfill.RepoBackfillStatus{Status: backfill.StatusComplete, Rev: rev, CompletedAt: at}
	rs.Rev = rev
	rs.UpdatedAt = at
	rs.LastAttemptedAt = at
	return rs
}

func runRetryScan(ctx context.Context, cmd *cli.Command) error {
	var (
		repos  = cmd.Int("repos")
		batch  = cmd.Int("batch")
		passes = cmd.Int("passes")
		share  = cmd.Float64("failed-share")
	)
	if share < 0 || share > 1 {
		return fmt.Errorf("--failed-share must be in [0, 1], got %g", share)
	}
	b, err := openBench(ctx, cmd)
	if err != nil {
		return err
	}
	defer b.close()

	lease := b.pg.NewLease()
	if err := lease.Acquire(ctx, time.Hour); err != nil {
		return err
	}
	sess := catalog.NewSession(catalog.SessionConfig{DB: b.pg, Epoch: lease.Epoch()})
	if _, err := sess.InitNamespace(ctx, catalog.Main, nil); err != nil {
		return err
	}
	meta := metapg.New(metapg.Config{DB: b.pg, Commit: func(ctx context.Context, ops []metastore.Op) error {
		_, err := sess.CommitMeta(ctx, ops)
		return err
	}})
	dbBefore, err := pgfixture.DatabaseBytes(ctx, b.pg)
	if err != nil {
		return err
	}

	fmt.Printf("loading %d repo/ rows (%.1f%% failed)\n", repos, 100*share)
	rng := rand.New(rand.NewPCG(cmd.Uint64("seed"), 0x5ca9))
	now := time.Now().UTC()
	var logical int64
	loadStart := time.Now()
	ops := make([]metastore.Op, 0, batch)
	for n := range repos {
		v, err := json.Marshal(repoRow(rng, now, share))
		if err != nil {
			return err
		}
		k := []byte("repo/" + didFor(uint64(n)))
		logical += int64(len(k) + len(v))
		ops = append(ops, metastore.Op{Kind: metastore.OpSet, Key: k, Value: v})
		if len(ops) == batch || n == repos-1 {
			if _, err := sess.CommitMeta(ctx, ops); err != nil {
				return err
			}
			ops = ops[:0]
		}
		if (n+1)%500_000 == 0 {
			fmt.Printf("  %d/%d loaded, %s\n", n+1, repos, time.Since(loadStart).Round(time.Second))
		}
	}
	load := time.Since(loadStart)
	dbAfter, err := pgfixture.DatabaseBytes(ctx, b.pg)
	if err != nil {
		return err
	}

	rows := [][2]string{
		{"repo rows", fmt.Sprint(repos)},
		{"load", fmt.Sprintf("%s (%.0f rows/s)", load.Round(time.Millisecond), float64(repos)/load.Seconds())},
		{"key+value bytes", fmt.Sprintf("%s (%.0f/row)", mib(float64(logical)), float64(logical)/float64(repos))},
		{"database growth", fmt.Sprintf("%s (%.0f/row)", mib(float64(dbAfter-dbBefore)), float64(dbAfter-dbBefore)/float64(repos))},
	}
	pop1 := float64(cmd.Uint64("pop1-repos"))
	scale := pop1 / float64(repos)
	rows = append(rows,
		[2]string{"pop1 key+value bytes", gib(float64(logical) * scale)},
		[2]string{"pop1 table bytes", gib(float64(dbAfter-dbBefore) * scale)},
	)
	for pass := range passes {
		raw, seen, err := rawRepoScan(ctx, meta)
		if err != nil {
			return err
		}
		if seen != repos {
			return fmt.Errorf("raw scan saw %d rows, loaded %d", seen, repos)
		}
		start := time.Now()
		counts, err := backfill.CountStatuses(meta)
		if err != nil {
			return err
		}
		decoded := time.Since(start)
		// CountStatuses skips a row it cannot decode, which would time less
		// work than scanDue does.
		if counts.Total != uint64(repos) || counts.Complete+counts.Failed != counts.Total {
			return fmt.Errorf("decode scan counted %+v, loaded %d", counts, repos)
		}
		p := fmt.Sprintf("pass %d ", pass+1)
		rows = append(rows,
			[2]string{p + "raw scan", scanString(raw, repos, scale)},
			[2]string{p + "decode scan", scanString(decoded, repos, scale) + fmt.Sprintf(", %d failed", counts.Failed)},
		)
	}
	printKV(os.Stdout, rows)
	return nil
}

// rawRepoScan iterates every repo/ row through the metastore, as scanDue
// does, without decoding.
func rawRepoScan(ctx context.Context, meta metastore.Store) (time.Duration, int, error) {
	start := time.Now()
	prefix := []byte("repo/")
	it, err := meta.NewIter(ctx, prefix, metastore.PrefixUpperBound(prefix))
	if err != nil {
		return 0, 0, err
	}
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		_ = it.Close()
		return 0, 0, err
	}
	if err := it.Close(); err != nil {
		return 0, 0, err
	}
	return time.Since(start), n, nil
}

func scanString(d time.Duration, repos int, scale float64) string {
	return fmt.Sprintf("%s (%.0f rows/s), pop1 %s", d.Round(time.Millisecond), float64(repos)/d.Seconds(),
		time.Duration(float64(d)*scale).Round(time.Second))
}

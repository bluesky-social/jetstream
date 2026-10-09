package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream/internal/jetstreamd"
	"github.com/bluesky-social/jetstream/internal/migrate"
	"github.com/urfave/cli/v3"
)

// migrationFlagCategory groups the migration flags in --help.
const migrationFlagCategory = "Migration to disaggregated storage"

// migrationFlags declares JETSTREAM_MIGRATION_*. Serve takes them so that
// a local-mode process can run the migrator.
func migrationFlags() []cli.Flag {
	def := jetstreamd.DefaultMigrationConfig()
	cat := migrationFlagCategory
	return []cli.Flag{
		&cli.BoolFlag{
			Name: "migrate-to-disaggregated", Category: cat, Usage: "Run the migrator in this local-mode process: copy the archive to the PostgreSQL and S3 that the storage flags name, keep it in step, and hand off on a jetstream migrate handoff request",
			Sources: cli.EnvVars("JETSTREAM_MIGRATE_TO_DISAGGREGATED"),
		},
		&cli.DurationFlag{
			Name: "migration-standby-backoff", Category: cat, Usage: "Disaggregated mode: wait this long after finding a catalog a migration still owns before trying for the lease again",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_STANDBY_BACKOFF"), Value: def.StandbyBackoff,
		},
		&cli.IntFlag{
			Name: "migration-segment-concurrency", Category: cat, Usage: "Sealed segments read and uploaded at once while seeding",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_SEGMENT_CONCURRENCY"), Value: def.SegmentConcurrency,
		},
		&cli.IntFlag{
			Name: "migration-read-bytes-per-sec", Category: cat, Usage: "Throttle on the migrator's reads of local segment files; 0 is unthrottled",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_READ_BYTES_PER_SEC"), Value: int(def.ReadBytesPerSec),
		},
		&cli.DurationFlag{
			Name: "migration-meta-flush-interval", Category: cat, Usage: "How often changed metadata keys are copied",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_META_FLUSH_INTERVAL"), Value: def.MetaFlushInterval,
		},
		&cli.IntFlag{
			Name: "migration-meta-batch-keys", Category: cat, Usage: "Metadata keys per copy transaction",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_META_BATCH_KEYS"), Value: def.MetaBatchKeys,
		},
		&cli.IntFlag{
			Name: "migration-dirty-max-keys", Category: cat, Usage: "Bound on the changed-key set; past it, a full metadata resync replaces it",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_DIRTY_MAX_KEYS"), Value: def.DirtyMaxKeys,
		},
		&cli.IntFlag{
			Name: "migration-tail-max-blocks-per-txn", Category: cat, Usage: "Active blocks shipped per transaction while tailing",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_TAIL_MAX_BLOCKS_PER_TXN"), Value: def.TailMaxBlocksPerTxn,
		},
		&cli.DurationFlag{
			Name: "migration-tail-poll-interval", Category: cat, Usage: "How often local ingest progress is sampled",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_TAIL_POLL_INTERVAL"), Value: def.TailPollInterval,
		},
		&cli.DurationFlag{
			Name: "migration-max-compaction-pause", Category: cat, Usage: "Refuse to hand off once local compaction has been paused this long",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_MAX_COMPACTION_PAUSE"), Value: def.MaxCompactionPause,
		},
		&cli.DurationFlag{
			Name: "migration-drain-spread", Category: cat, Usage: "After the handoff, close subscribers spread over this window",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_DRAIN_SPREAD"), Value: def.DrainSpread,
		},
		&cli.DurationFlag{
			Name: "migration-handoff-timeout", Category: cat, Usage: "Bound on stopping local ingest at handoff; past it the handoff reverts",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_HANDOFF_TIMEOUT"), Value: def.HandoffTimeout,
		},
		&cli.DurationFlag{
			Name: "migration-verify-interval", Category: cat, Usage: "How often a full metadata resync runs while tailing",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_VERIFY_INTERVAL"), Value: def.VerifyInterval,
		},
		&cli.IntFlag{
			Name: "migration-handoff-max-lag-seqs", Category: cat, Usage: "Refuse to hand off while the replica trails local ingest by more seqs than this",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_HANDOFF_MAX_LAG_SEQS"), Value: int(def.HandoffMaxLagSeqs),
		},
		&cli.DurationFlag{
			Name: "migration-handoff-max-resync-age", Category: cat, Usage: "Refuse to hand off unless a full metadata resync finished this recently",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_HANDOFF_MAX_RESYNC_AGE"), Value: def.HandoffMaxDiffAge,
		},
		&cli.BoolFlag{
			Name: "migration-handoff-full-verify", Category: cat, Usage: "Compare all metadata while ingest is stopped at handoff; lengthens the ingest pause by a full read of both stores",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_HANDOFF_FULL_VERIFY"),
		},
		&cli.DurationFlag{
			Name: "migration-control-interval", Category: cat, Usage: "How often operator requests are polled and the status published",
			Sources: cli.EnvVars("JETSTREAM_MIGRATION_CONTROL_INTERVAL"), Value: def.ControlInterval,
		},
	}
}

func migrationConfigFromCommand(cmd *cli.Command) jetstreamd.MigrationConfig {
	return jetstreamd.MigrationConfig{
		Enabled:             cmd.Bool("migrate-to-disaggregated"),
		StandbyBackoff:      cmd.Duration("migration-standby-backoff"),
		SegmentConcurrency:  cmd.Int("migration-segment-concurrency"),
		ReadBytesPerSec:     int64(cmd.Int("migration-read-bytes-per-sec")),
		MetaFlushInterval:   cmd.Duration("migration-meta-flush-interval"),
		MetaBatchKeys:       cmd.Int("migration-meta-batch-keys"),
		DirtyMaxKeys:        cmd.Int("migration-dirty-max-keys"),
		TailMaxBlocksPerTxn: cmd.Int("migration-tail-max-blocks-per-txn"),
		TailPollInterval:    cmd.Duration("migration-tail-poll-interval"),
		MaxCompactionPause:  cmd.Duration("migration-max-compaction-pause"),
		DrainSpread:         cmd.Duration("migration-drain-spread"),
		HandoffTimeout:      cmd.Duration("migration-handoff-timeout"),
		VerifyInterval:      cmd.Duration("migration-verify-interval"),
		HandoffMaxLagSeqs:   uint64(max(cmd.Int("migration-handoff-max-lag-seqs"), 0)),
		HandoffMaxDiffAge:   cmd.Duration("migration-handoff-max-resync-age"),
		HandoffFullVerify:   cmd.Bool("migration-handoff-full-verify"),
		ControlInterval:     cmd.Duration("migration-control-interval"),
	}
}

// migrateCommand is the operator's side of a migration (migration plan
// §6.7, §8).
func migrateCommand() *cli.Command {
	// A flag holds its parsed value, so each command gets its own.
	wait := func() cli.Flag {
		return &cli.DurationFlag{Name: "wait", Usage: "How long to wait for the migrator's answer", Value: 10 * time.Minute}
	}
	return &cli.Command{
		Name:  "migrate",
		Usage: "Drive a running migration from local to disaggregated storage",
		Commands: []*cli.Command{
			{
				Name:   "status",
				Usage:  "Print the migration's state, from the catalog and from the migrator's last report",
				Flags:  storageInitFlags(),
				Action: runMigrateStatus,
			},
			{
				Name:   "handoff",
				Usage:  "Ask the migrator to stop local ingest, ship the rest, and hand the archive to the disaggregated pods. Flip the public routes to the pods first.",
				Flags:  append(storageInitFlags(), wait()),
				Action: runMigrateRequest(migrate.ActionHandoff),
			},
			{
				Name:   "abort",
				Usage:  "Ask the migrator to abandon an unfinished migration; local compaction resumes",
				Flags:  append(storageInitFlags(), wait()),
				Action: runMigrateRequest(migrate.ActionAbort),
			},
			{
				Name:  "reclaim",
				Usage: "Emergency rollback after a handoff: with every disaggregated pod and the local process stopped, mark the catalog reverted and let the local archive resume past every seq the pods assigned",
				Flags: append(storageInitFlags(),
					&cli.StringFlag{Name: "data-dir", Usage: "The local archive's data directory", Required: true},
					&cli.IntFlag{Name: "margin", Usage: "Seqs left between the catalog's last seq and the first the local archive assigns", Value: jetstreamd.DefaultReclaimMargin},
				),
				Action: runMigrateReclaim,
			},
		},
	}
}

func signalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
}

func runMigrateStatus(ctx context.Context, cmd *cli.Command) error {
	ctx, stop := signalContext(ctx)
	defer stop()
	cfg, _, err := storageConfigFromCommand(cmd)
	if err != nil {
		return err
	}
	rep, err := jetstreamd.MigrationStatus(ctx, cfg)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(cmd.Root().Writer)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func runMigrateRequest(action string) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		ctx, stop := signalContext(ctx)
		defer stop()
		cfg, _, err := storageConfigFromCommand(cmd)
		if err != nil {
			return err
		}
		result, err := jetstreamd.RequestMigration(ctx, cfg, action, cmd.Duration("wait"))
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(cmd.Root().Writer, "%s: %s\n", action, result); err != nil {
			return err
		}
		if want := map[string]string{migrate.ActionHandoff: "done", migrate.ActionAbort: "aborted"}[action]; !strings.HasPrefix(result, want) {
			return fmt.Errorf("migrate %s did not complete: %s", action, result)
		}
		return nil
	}
}

func runMigrateReclaim(ctx context.Context, cmd *cli.Command) error {
	ctx, stop := signalContext(ctx)
	defer stop()
	cfg, _, err := storageConfigFromCommand(cmd)
	if err != nil {
		return err
	}
	res, err := jetstreamd.ReclaimLocal(ctx, cfg, cmd.String("data-dir"), nil, uint64(max(cmd.Int("margin"), 0)))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(cmd.Root().Writer,
		"reclaimed: catalog reverted at seq %d; local archive resumes at seq %d (seqs [%d,%d) become a vacancy). Start it with JETSTREAM_MIGRATE_TO_DISAGGREGATED unset.\n",
		res.CatalogNext, res.ResumeSeq, res.LocalNext, res.ResumeSeq)
	return err
}

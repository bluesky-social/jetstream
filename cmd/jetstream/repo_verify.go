package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
	"github.com/urfave/cli/v3"
)

var errRepoVerifyMismatch = errors.New("repo verification mismatch")

func verifyRepoCommand() *cli.Command {
	return &cli.Command{
		Name:      "verify-repo",
		Usage:     "Compare a local repo reconstruction against the authoritative relay repo root",
		ArgsUsage: "<did>",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "data-dir",
				Usage:   "Path to the data directory; repo events are read from <data-dir>/segments and <data-dir>/backfill/live_segments",
				Sources: cli.EnvVars("JETSTREAM_DATA_DIR"),
				Value:   "./data",
			},
			&cli.StringFlag{
				Name:    "relay-url",
				Usage:   "Base URL of the upstream relay",
				Sources: cli.EnvVars("JETSTREAM_RELAY_URL"),
				Value:   "https://bsky.network",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args()
			if args.Len() != 1 {
				return fmt.Errorf("verify-repo: expected exactly one DID argument, got %d", args.Len())
			}

			report, err := repoexport.Verify(ctx, repoexport.VerifyConfig{
				DataDir:  cmd.String("data-dir"),
				DID:      args.First(),
				RelayURL: cmd.String("relay-url"),
			})
			if err != nil {
				return err
			}
			return renderRepoVerifyReport(cmd.Root().Writer, report)
		},
	}
}

func renderRepoVerifyReport(w io.Writer, report repoexport.VerifyReport) error {
	bw := &errWriter{w: w}

	if report.Match {
		bw.printf("repo verification: match\n")
	} else {
		bw.printf("repo verification: mismatch\n")
	}
	bw.printf("did: %s\n", report.DID)
	bw.printf("authoritative_rev: %s\n", report.AuthoritativeRev)
	bw.printf("authoritative_root: %s\n", report.AuthoritativeRoot)
	bw.printf("local_latest_rev: %s\n", report.LocalLatestRev)
	bw.printf("local_root: %s\n", report.LocalRoot)
	bw.printf("local_records: %d\n", report.LocalRecordCount)
	if report.Message != "" {
		bw.printf("message: %s\n", report.Message)
	}
	if bw.err != nil {
		return bw.err
	}
	if !report.Match {
		return errRepoVerifyMismatch
	}
	return nil
}

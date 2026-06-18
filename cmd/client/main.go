// Command client drives the jetstream Go client against a running server: it
// live-tails the firehose or runs a filtered backfill that cuts over to live,
// printing decoded events or throughput stats. The legacy raw-websocket load
// tester is available under the "loadtest" subcommand.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {
	if err := newApp().Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "jetstream-client:", err)
		os.Exit(1)
	}
}

func newApp() *cli.Command {
	cmd := subscribeCommand()
	cmd.Name = "jetstream-client"
	cmd.Usage = "Drive the jetstream Go client (live tail or backfill+cutover)"
	cmd.Commands = []*cli.Command{loadtestCommand()}
	return cmd
}

// Performs a full network replay with basic options, printing some sample events as it goes. This may take a while!
//
// Be sure to set the JETSTREAM_API_KEY environment variable if your server requires auth.
//
// See the documentation for more information https://pkg.go.dev/github.com/bluesky-social/jetstream
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/bluesky-social/jetstream"
)

type CLIArgs struct {
	Host   string
	APIKey string
}

func main() {
	args := CLIArgs{}
	flag.StringVar(&args.Host, "host", "https://jetstream.us-east.bsky.network", "Jetstream host to which the process will connect")
	flag.Parse()

	args.APIKey = os.Getenv("JETSTREAM_API_KEY")

	if err := run(context.Background(), &args); err != nil {
		slog.Error("an error occurred while running the jetstream example", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args *CLIArgs) error {
	// Declare our client connection options (see the docs for more)
	opts := []jetstream.Option{
		jetstream.WithAfterSeq(0), // start from the beginning of time
	}

	if args.APIKey != "" {
		opts = append(opts, jetstream.WithAPIToken(args.APIKey))
	}

	client, err := jetstream.Subscribe(args.Host, opts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer client.Close()

	// Print an event every so often
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastEvent jetstream.Event
	for batch, err := range client.Events(ctx) {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("received fatal error: %w", err)
			}

			// You should handle other errors gracefully
			fmt.Printf("got an error: %v\n", err)
			continue
		}

		// These are all the events that are downloaded throughout the course of the replay.
		// Do with them what you wish!
		for _, event := range batch.Events() {
			lastEvent = event
		}

		// You should persist this cursor to your database of choice
		// so you can resume later if your process restarts
		// batch.LastCursor()

		// Print events every so often just to show we're doing something
		select {
		case <-ticker.C:
			if err := printLastEvent(lastEvent); err != nil {
				return fmt.Errorf("failed to marshal event: %w", err)
			}
		default:
		}
	}

	return nil
}

func printLastEvent(event jetstream.Event) error {
	buf, err := json.Marshal(event)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", buf)
	return nil
}

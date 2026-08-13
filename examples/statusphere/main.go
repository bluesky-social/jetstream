// Performs a full replay of all the emojis from the statusphere example app.
//
// Be sure to set the JETSTREAM_API_KEY environment variable if your server requires auth.
//
// See the documentation for more information https://pkg.go.dev/github.com/bluesky-social/jetstream
//
// See also the statusphere example repo https://github.com/bluesky-social/statusphere-example-app/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/bluesky-social/jetstream"
	"github.com/jcalabro/atmos/api/statusphere"
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
		slog.Error("an error occurred while running the jetstream statusphere example", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args *CLIArgs) error {
	// Declare our client connection options (see the docs for more)
	opts := []jetstream.Option{
		// start from the beginning of time
		jetstream.WithAfterSeq(0),

		// Using TypedEvents can result in MUCH faster client decode performance,
		// if you know the single collection you care about ahead of time. To do so,
		// set `WithRawRecords` and use the alternate `TypedEvents` implementation
		// like we do below. This is a super specialized implementation and is pretty
		// inflexible. If you are unsure what to use, don't use `TypedEvents`; just go
		// with the basic `jetstream.Subscribe` implemenation.
		jetstream.WithRawRecords(),
	}

	if args.APIKey != "" {
		opts = append(opts, jetstream.WithAPIToken(args.APIKey))
	}

	// Create a client connection pool
	client, err := jetstream.Subscribe(args.Host, opts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer client.Close() // Don't forget to clean up!

	// Start our client loop
	for batch, err := range jetstream.TypedEvents[statusphere.StatusphereStatus](ctx, client, "xyz.statusphere.status") {
		if err != nil {
			if errors.Is(err, jetstream.ErrFatal) {
				return fmt.Errorf("received fatal error: %w", err)
			}

			// You should handle other errors gracefully, depending
			// on what your application needs
			fmt.Printf("got an error: %v\n", err)
			continue
		}

		// Print the emojis folks have set
		for _, event := range batch.Events() {
			if err := event.DecodeErr; err != nil || event.Record == nil {
				// You should handle these cases gracefully, but we'll skip it for this example
				continue
			}

			// `event` now represents a strongly typed instance of xyz.statusphere.status
			fmt.Printf("%s had the status %s\n", event.Event.DID, event.Record.Status)
		}

		// You should persist this cursor to your database of choice
		// so you can resume later if your process restarts
		// batch.LastCursor()
	}

	return nil
}

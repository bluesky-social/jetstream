// Package version exposes build metadata that is injected at link time via
// -ldflags. Defaults are sentinel values so an unstamped binary still works
// (e.g. `go run ./cmd/jetstream`) and is obviously distinguishable from a
// proper release build.
package version

// These variables are set via:
//
//	go build -ldflags "-X github.com/bluesky-social/jetstream-v2/internal/version.Version=v1.2.3 ..."
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info bundles the build metadata for cheap, allocation-free passing.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Get returns a snapshot of the current build info.
func Get() Info {
	return Info{Version: Version, Commit: Commit, Date: Date}
}

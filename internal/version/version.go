// Package version exposes link-time build metadata. Defaults identify
// unstamped development builds.
package version

// These variables are set via:
//
//	go build -ldflags "-X github.com/bluesky-social/jetstream/internal/version.Version=v1.2.3 ..."
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

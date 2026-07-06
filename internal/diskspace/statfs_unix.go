//go:build !windows

package diskspace

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// FreeBytes returns bytes available to this process at path's filesystem.
func FreeBytes(path string) (uint64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("diskspace: statfs %s: %w", path, err)
	}
	return st.Bavail * uint64(st.Bsize), nil
}

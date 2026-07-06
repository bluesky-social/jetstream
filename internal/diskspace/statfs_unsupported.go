//go:build windows

package diskspace

import "fmt"

// FreeBytes returns bytes available to this process at path's filesystem.
func FreeBytes(path string) (uint64, error) {
	return 0, fmt.Errorf("diskspace: statfs %s: unsupported platform", path)
}

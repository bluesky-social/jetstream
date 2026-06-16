// Package format provides shared human-readable formatters used by the
// CLI renderers, the load-test client, and the web status page. These
// were previously duplicated (and drifting) across those packages.
package format

import (
	"fmt"
	"strconv"
	"strings"
)

// Int renders n with comma separators ("1,234,567"). Zero renders as "0".
func Int(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

// Bytes formats n as a base-1024 human-readable size with two decimal
// places ("1.50 KiB"). Values below 1024 render as plain bytes ("512 B").
// Suffixes cap at PiB.
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), suffixes[exp])
}

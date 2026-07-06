package ingest

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
)

// WrapDiskFull converts an ENOSPC-rooted persistence error into the fatal
// disk-full operator message; every other error (including nil) passes
// through unchanged. All segment persistence paths — the active writer, the
// pebble durable-batch commit, compaction rewrite, and import patch — must
// route disk-full errors through this one wrapper so operators see a single,
// actionable message regardless of which path hit the full disk.
func WrapDiskFull(dataDir, op string, err error) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	return fmt.Errorf("fatal persistence error: disk full while %s (data_dir=%q): free space or move the data directory, then restart jetstream: %w",
		op, dataDir, err)
}

func (c Config) wrapSegmentPersistenceError(op string, err error) error {
	dataDir := c.DataDir
	if dataDir == "" {
		dataDir = filepath.Dir(c.SegmentsDir)
	}
	return WrapDiskFull(dataDir, op, err)
}

func (w *Writer) wrapSegmentPersistenceError(op string, err error) error {
	if w == nil {
		return err
	}
	return w.cfg.wrapSegmentPersistenceError(op, err)
}

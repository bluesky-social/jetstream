package ingest

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
)

func (c Config) wrapSegmentPersistenceError(op string, err error) error {
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	dataDir := c.DataDir
	if dataDir == "" {
		dataDir = filepath.Dir(c.SegmentsDir)
	}
	return fmt.Errorf("fatal persistence error: disk full while %s (data_dir=%q): free space or move the data directory, then restart jetstream: %w",
		op, dataDir, err)
}

func (w *Writer) wrapSegmentPersistenceError(op string, err error) error {
	if w == nil {
		return err
	}
	return w.cfg.wrapSegmentPersistenceError(op, err)
}

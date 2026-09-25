package orchestrator

import (
	"context"
	"fmt"

	"github.com/bluesky-social/jetstream/internal/metastore"
)

const (
	compactionWatermarkKey = "compaction/seq"
	compactionWatermarkV1  = 0x01
)

func loadCompactionWatermark(s metastore.Store) (uint64, bool, error) {
	v, ok, err := metastore.GetVersionedUint64LE(context.Background(), s, compactionWatermarkKey, compactionWatermarkV1)
	if err != nil {
		return 0, false, fmt.Errorf("orchestrator: compaction: load watermark: %w", err)
	}
	return v, ok, nil
}

func saveCompactionWatermark(s metastore.Store, seq uint64) error {
	if err := metastore.SetVersionedUint64LE(context.Background(), s, compactionWatermarkKey, compactionWatermarkV1, seq); err != nil {
		return fmt.Errorf("orchestrator: compaction: save watermark: %w", err)
	}
	return nil
}

func initCompactionWatermarkFloor(s metastore.Store, nextSeq uint64) error {
	_, ok, err := loadCompactionWatermark(s)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if nextSeq == 0 {
		return saveCompactionWatermark(s, 0)
	}
	return saveCompactionWatermark(s, nextSeq-1)
}

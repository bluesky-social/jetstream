package orchestrator

import (
	"fmt"

	"github.com/bluesky-social/jetstream-v2/internal/store"
)

const (
	compactionWatermarkKey = "compaction/seq"
	compactionWatermarkV1  = 0x01
)

func loadCompactionWatermark(s *store.Store) (uint64, bool, error) {
	v, ok, err := s.GetVersionedUint64LE(compactionWatermarkKey, compactionWatermarkV1)
	if err != nil {
		return 0, false, fmt.Errorf("orchestrator: compaction: load watermark: %w", err)
	}
	return v, ok, nil
}

func saveCompactionWatermark(s *store.Store, seq uint64) error {
	if err := s.SetVersionedUint64LE(compactionWatermarkKey, compactionWatermarkV1, seq); err != nil {
		return fmt.Errorf("orchestrator: compaction: save watermark: %w", err)
	}
	return nil
}

func initCompactionWatermarkFloor(s *store.Store, nextSeq uint64) error {
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

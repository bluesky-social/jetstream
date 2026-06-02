package world

import (
	"fmt"
	"io"

	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/mst"
)

// ExportRepoCAR writes the account's persisted repo head as a CAR. Unlike
// repo.ExportCAR, this does not sign a fresh commit; getRepo must expose the
// same head CID and rev that listRepos advertised.
func (w *World) ExportRepoCAR(idx int, dst io.Writer) error {
	state, err := w.loadState(idx)
	if err != nil {
		return err
	}
	if !state.CommitCID.Defined() {
		return fmt.Errorf("world: account %d has no persisted commit", idx)
	}

	store := &pebbleStore{db: w.db, idx: idx}
	commitData, err := store.GetBlock(state.CommitCID)
	if err != nil {
		return fmt.Errorf("world: load commit block for account %d: %w", idx, err)
	}
	blocks := []car.Block{{CID: state.CommitCID, Data: commitData}}
	seen := map[string]struct{}{string(state.CommitCID.Bytes()): {}}
	if state.DataCID.Defined() {
		if err := collectRepoCARBlocks(store, state.DataCID, &blocks, seen); err != nil {
			return fmt.Errorf("world: collect repo blocks for account %d: %w", idx, err)
		}
	}
	return car.WriteAll(dst, []cbor.CID{state.CommitCID}, blocks)
}

func collectRepoCARBlocks(store mst.BlockStore, cid cbor.CID, blocks *[]car.Block, seen map[string]struct{}) error {
	key := string(cid.Bytes())
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}

	data, err := store.GetBlock(cid)
	if err != nil {
		return err
	}
	*blocks = append(*blocks, car.Block{CID: cid, Data: data})

	node, err := mst.DecodeNodeData(data)
	if err != nil {
		return nil //nolint:nilerr // expected: non-MST blocks (leaf record blocks) fail to decode and have no children to collect
	}
	if node.Left.HasVal() {
		if err := collectRepoCARBlocks(store, node.Left.Val(), blocks, seen); err != nil {
			return err
		}
	}
	for _, entry := range node.Entries {
		if err := collectRepoCARBlocks(store, entry.Value, blocks, seen); err != nil {
			return err
		}
		if entry.Right.HasVal() {
			if err := collectRepoCARBlocks(store, entry.Right.Val(), blocks, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

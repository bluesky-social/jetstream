package jetstreamd

import (
	"github.com/bluesky-social/jetstream-v2/internal/manifest"
	"github.com/bluesky-social/jetstream-v2/internal/repoexport"
)

// manifestSelector adapts the in-memory *manifest.Manifest to the
// repoexport.Selector interface the /status verification path consumes.
// The manifest already holds every sealed segment's DID blooms resident,
// so block pruning happens entirely in memory; reconstruction then opens
// only the few segments an account actually touches instead of every
// sealed segment on disk.
//
// Keeping repoexport behind an interface (rather than importing manifest
// directly) preserves the package boundary: manifest never depends on
// repoexport, and repoexport never depends on manifest.
type manifestSelector struct {
	m *manifest.Manifest
}

func newManifestSelector(m *manifest.Manifest) repoexport.Selector {
	return manifestSelector{m: m}
}

func (s manifestSelector) SelectBlocksForDID(did string) ([]repoexport.BlockSelection, error) {
	sel, err := s.m.SelectBlocksForDID(did)
	if err != nil {
		return nil, err
	}
	out := make([]repoexport.BlockSelection, len(sel))
	for i := range sel {
		out[i] = repoexport.BlockSelection{
			Path:   sel[i].Path,
			Blocks: sel[i].Blocks,
		}
	}
	return out, nil
}

func (s manifestSelector) ActiveSegmentPaths() ([]string, error) {
	return s.m.ActiveSegmentPaths()
}

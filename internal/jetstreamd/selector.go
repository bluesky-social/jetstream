package jetstreamd

import (
	"github.com/bluesky-social/jetstream/internal/catalog"
	"github.com/bluesky-social/jetstream/internal/manifest"
	"github.com/bluesky-social/jetstream/internal/repoexport"
)

// manifestSelector adapts the in-memory *manifest.Manifest to the
// repoexport.Selector interface the /status verification path consumes.
// The manifest already holds every sealed main segment's DID blooms
// resident, so block pruning happens entirely in memory; reconstruction
// then decodes only the few blocks an account actually touches.
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

// SelectBlocksForDID reports every segment the manifest holds as checked,
// with its candidate blocks. The manifest covers main only; other
// namespaces come back unchecked for the footer fallback.
func (s manifestSelector) SelectBlocksForDID(ns catalog.Namespace, did string) (repoexport.Selection, error) {
	if ns != catalog.Main {
		return nil, nil
	}
	// Take the resident set before selecting: a segment absorbed between
	// the two calls is then left unchecked and decoded in full, rather than
	// marked checked with no candidates.
	sel := repoexport.Selection{}
	for idx := range s.m.SegmentChecksums() {
		sel[idx] = nil
	}
	picks, err := s.m.SelectBlocksForDID(did)
	if err != nil {
		return nil, err
	}
	for _, p := range picks {
		sel[p.Idx] = p.Blocks
	}
	return sel, nil
}

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCatalogPatchesCarryContext keeps every mutant a content-anchored diff.
// A zero-context hunk is anchored only by its line number, so as the target
// file changes it lands on an identical line in another function (m027 and
// m060 did, and kept reporting KILLED) or reverts into the wrong spot. The
// driver applies patches without --unidiff-zero, so such a patch would report
// STALE there; this catches it in the default test run instead.
func TestCatalogPatchesCarryContext(t *testing.T) {
	t.Parallel()
	patches, err := filepath.Glob("../mutants/*.patch")
	if err != nil {
		t.Fatal(err)
	}
	if len(patches) == 0 {
		t.Fatal("no mutant patches found")
	}
	for _, p := range patches {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range zeroContextHunks(string(data)) {
			t.Errorf("%s: hunk %q has no context lines; regenerate it with git diff -U3", filepath.Base(p), h)
		}
	}
}

// zeroContextHunks returns the headers of the hunks in patch that carry no
// context lines. The metadata header before the first "diff --git" is
// skipped, since its indented YAML block lines look like context.
func zeroContextHunks(patch string) []string {
	_, body, ok := strings.Cut(patch, "\ndiff --git ")
	if !ok {
		return []string{"(no diff)"}
	}
	var bad []string
	header, context := "", false
	flush := func() {
		if header != "" && !context {
			bad = append(bad, header)
		}
		header, context = "", false
	}
	for line := range strings.SplitSeq(body, "\n") {
		switch {
		case strings.HasPrefix(line, "@@"):
			flush()
			header = line
		case strings.HasPrefix(line, "diff --git "):
			flush()
		case header != "" && strings.HasPrefix(line, " "):
			context = true
		}
	}
	flush()
	return bad
}

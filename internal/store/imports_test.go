package store_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const storeImportPath = "github.com/bluesky-social/jetstream/internal/store"

// Metadata access goes through metastore.Store so the disaggregated backend
// can replace Pebble without touching call sites (plan S1.6). A new importer
// of this package would bypass that seam.
var allowedStoreImporters = map[string]bool{
	"internal/store":                 true,
	"internal/metastore/pebblestore": true,
}

func TestOnlyPebblestoreImportsStore(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)

	fset := token.NewFileSet()
	var offenders []string
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if p == storeImportPath && !allowedStoreImporters[filepath.ToSlash(rel)] {
				offenders = append(offenders, path)
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, offenders, "import internal/metastore instead of internal/store")
}

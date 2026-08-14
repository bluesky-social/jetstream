package ci_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuzzWorkflowShardsEveryTarget(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	workflow := readFile(t, filepath.Join(root, ".github/workflows/ci-scheduled.yml"))

	re := regexp.MustCompile(`\{ package: "([^"]+)", target: (Fuzz[[:alnum:]_]+) \}`)
	got := make([]string, 0)
	for _, match := range re.FindAllStringSubmatch(workflow, -1) {
		got = append(got, match[1]+"/"+match[2])
	}
	sort.Strings(got)

	want := fuzzTargets(t, root)
	require.Equal(t, want, got, "the scheduled fuzz matrix must contain every fuzz target exactly once")
	require.NotContains(t, workflow, "just fuzz 300s", "scheduled fuzzing must not run every target on one runner")
	require.Contains(t, workflow, "just fuzz-target 300s", "each matrix leg must run one target")
}

func TestOracleWorkflowUsesStableSingleSeedShards(t *testing.T) {
	t.Parallel()

	root := repoRoot(t)
	workflow := readFile(t, filepath.Join(root, ".github/workflows/oracle-scheduled.yml"))
	justfile := readFile(t, filepath.Join(root, "justfile"))

	require.Contains(t, workflow, "fail-fast: false")
	require.Contains(t, workflow, "matrix.seed")
	require.Contains(t, workflow, `just oracle-sweep 1 "" "$ORACLE_SEED"`)
	require.Contains(t, workflow, `just oracle-sweep 1 race "$ORACLE_SEED"`)
	require.Contains(t, workflow, `github.event.inputs.seeds || '5'`)
	require.Contains(t, workflow, `github.event.inputs.race_seeds || '1'`)
	require.Contains(t, justfile, `oracle-sweep SEEDS="10" RACE="" FIXED_SEED=""`)
	require.Contains(t, justfile, `fixed_seed="{{FIXED_SEED}}"`)
}

func TestInfrastructureRetryIsNarrowAndOneShot(t *testing.T) {
	t.Parallel()

	workflow := readFile(t, filepath.Join(repoRoot(t), ".github/workflows/retry-infrastructure-failures.yml"))

	require.Contains(t, workflow, "workflow_run:")
	require.Contains(t, workflow, "run_attempt == 1")
	require.Contains(t, workflow, "workflow_run.event == 'schedule'")
	require.Contains(t, workflow, "workflow_run.event == 'workflow_dispatch'")
	require.Contains(t, workflow, "workflow_run.repository.full_name == github.repository")
	require.Contains(t, workflow, "--paginate --slurp")
	require.Contains(t, workflow, "rerun-failed-jobs")
	require.Contains(t, workflow, "The hosted runner lost communication with the server")
	require.Contains(t, workflow, "The runner has received a shutdown signal")
	timeoutVeto := strings.Index(workflow, "has exceeded the maximum execution time")
	runnerLoss := strings.Index(workflow, "The hosted runner lost communication with the server")
	require.NotEqual(t, -1, timeoutVeto)
	require.Less(t, timeoutVeto, runnerLoss, "job timeout evidence must veto infrastructure retry")
	require.NotContains(t, workflow, "actions/checkout", "the privileged retry workflow must not execute repository code")
	require.NotContains(t, workflow, "pull_request")
}

func fuzzTargets(t *testing.T, root string) []string {
	t.Helper()
	var targets []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		dir, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := "."
		if dir != "." {
			pkg = "./" + filepath.ToSlash(dir)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Fuzz") {
				targets = append(targets, pkg+"/"+fn.Name.Name)
			}
		}
		return nil
	})
	require.NoError(t, err)
	sort.Strings(targets)
	return targets
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return root
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(contents)
}

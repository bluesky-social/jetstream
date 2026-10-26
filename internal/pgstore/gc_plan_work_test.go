package pgstore_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGCPlanWorkAccounting(t *testing.T) {
	t.Parallel()
	plan := json.RawMessage(`[{"Plan":{"Node Type":"Result","Actual Rows":1,"Actual Loops":1,"Shared Hit Blocks":999,"Plans":[
		{"Node Type":"Limit","Subplan Name":"CTE page","Actual Rows":4,"Actual Loops":1,"Plans":[
			{"Node Type":"Index Scan","Relation Name":"objects","Alias":"objects","Index Name":"objects_pkey","Actual Rows":4,"Actual Loops":1,"Rows Removed by Filter":6,"Shared Hit Blocks":8,"Shared Read Blocks":2}]},
		{"Node Type":"Nested Loop","Actual Rows":4,"Actual Loops":1,"Shared Hit Blocks":900,"Plans":[
			{"Node Type":"Index Scan","Relation Name":"objects","Alias":"o","Index Name":"objects_pkey","Actual Rows":1,"Actual Loops":4,"Rows Removed by Filter":2,"Rows Removed by Index Recheck":1,"Shared Hit Blocks":12,"Shared Read Blocks":0},
			{"Node Type":"Index Only Scan","Relation Name":"generation_blocks","Alias":"g","Index Name":"generation_blocks_object","Actual Rows":0.5,"Actual Loops":4,"Heap Fetches":3,"Shared Hit Blocks":5,"Shared Read Blocks":1}]},
		{"Node Type":"Mystery Scan","Actual Rows":0,"Actual Loops":0}]}}]`)
	work, err := analyzeGCPlanWork(plan)
	require.NoError(t, err)
	require.Len(t, work, 3)
	require.Equal(t, "candidate", work[0].Scope)
	require.Equal(t, float64(10), work[0].Rows)
	require.Equal(t, "target", work[1].Scope)
	require.Equal(t, float64(16), work[1].Rows)
	require.Equal(t, float64(4), work[1].Loops)
	require.Equal(t, "reference", work[2].Scope)
	require.Equal(t, float64(2), work[2].Rows)
	var hits, reads float64
	for _, scan := range work {
		hits += scan.SharedHitBlocks
		reads += scan.SharedReadBlocks
	}
	require.Equal(t, float64(25), hits, "ancestor buffers and loops must not multiply relation buffer counts")
	require.Equal(t, float64(3), reads)
}

func TestGCPlanWorkBitmap(t *testing.T) {
	t.Parallel()
	plan := json.RawMessage(`[{"Plan":{"Node Type":"Bitmap Heap Scan","Relation Name":"objects","Alias":"o","Actual Rows":2,"Actual Loops":2,"Rows Removed by Filter":3,"Rows Removed by Index Recheck":5,"Shared Hit Blocks":25,"Shared Read Blocks":4,"Plans":[
		{"Node Type":"BitmapAnd","Actual Rows":0,"Actual Loops":2,"Shared Hit Blocks":10,"Shared Read Blocks":1,"Plans":[
			{"Node Type":"Bitmap Index Scan","Index Name":"objects_gc","Actual Rows":5000,"Actual Loops":2,"Shared Hit Blocks":8,"Shared Read Blocks":1},
			{"Node Type":"Bitmap Index Scan","Index Name":"objects_pkey","Actual Rows":2,"Actual Loops":2,"Shared Hit Blocks":2,"Shared Read Blocks":0}]}]}}]`)
	work, err := analyzeGCPlanWork(plan)
	require.NoError(t, err)
	require.Len(t, work, 3)
	require.Equal(t, float64(20), work[0].Rows)
	require.Equal(t, float64(10_000), work[1].Rows, "broad index work must survive a selective bitmap intersection")
	require.Equal(t, "objects", work[1].Relation)
	require.Equal(t, "target", work[1].Scope)
	require.Equal(t, "objects_gc", work[1].IndexName)
	var hits, reads float64
	for _, scan := range work {
		hits += scan.SharedHitBlocks
		reads += scan.SharedReadBlocks
	}
	require.Equal(t, float64(25), hits, "bitmap subtree buffers are already included in the heap scan")
	require.Equal(t, float64(4), reads)
}

func TestGCPlanWorkNestedReferences(t *testing.T) {
	t.Parallel()
	plan := json.RawMessage(`[{"Plan":{"Node Type":"ModifyTable","Relation Name":"objects","Alias":"o","Actual Rows":1,"Actual Loops":1,"Shared Hit Blocks":120,"Shared Read Blocks":10,"Plans":[
		{"Node Type":"Seq Scan","Relation Name":"objects","Alias":"o","Actual Rows":1,"Actual Loops":1,"Rows Removed by Filter":999,"Shared Hit Blocks":100,"Shared Read Blocks":10,"Plans":[
			{"Node Type":"Seq Scan","Relation Name":"hot_batches","Alias":"h","Actual Rows":500,"Actual Loops":1,"Shared Hit Blocks":40,"Shared Read Blocks":3}]}]}}]`)
	work, err := analyzeGCPlanWork(plan)
	require.NoError(t, err)
	require.Len(t, work, 2, "ModifyTable identifies its target but is not another relation scan")
	require.Equal(t, float64(1000), work[0].Rows, "a small UPDATE result must not hide a full target scan")
	require.Equal(t, float64(60), work[0].SharedHitBlocks)
	require.Equal(t, float64(7), work[0].SharedReadBlocks)
	require.Equal(t, "reference", work[1].Scope)
	require.Equal(t, float64(500), work[1].Rows)
}

func TestGCPlanWorkCandidateContexts(t *testing.T) {
	t.Parallel()
	for _, subplan := range []string{"CTE page", "CTE c", ""} {
		t.Run(subplan, func(t *testing.T) {
			t.Parallel()
			plan := json.RawMessage(fmt.Sprintf(`[{"Plan":{"Node Type":"Limit","Subplan Name":%q,"Actual Rows":1,"Actual Loops":1,"Plans":[
				{"Node Type":"Index Scan","Relation Name":"objects","Alias":"objects","Index Name":"objects_gc","Actual Rows":1,"Actual Loops":1,"Shared Hit Blocks":2,"Shared Read Blocks":0}]}}]`, subplan))
			work, err := analyzeGCPlanWork(plan)
			require.NoError(t, err)
			require.Len(t, work, 1)
			require.Equal(t, "candidate", work[0].Scope)
		})
	}
}

func TestGCPlanWorkRejectsUnknownEvidence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, plan string
	}{
		{"malformed", `[`},
		{"missing root", `[{}]`},
		{"multiple statements", `[{"Plan":{}},{"Plan":{}}]`},
		{"unknown scan", `[{"Plan":{"Node Type":"Foreign Scan","Relation Name":"objects","Alias":"o","Actual Rows":1,"Actual Loops":1}}]`},
		{"unknown wrapper", `[{"Plan":{"Node Type":"Custom Scan","Actual Rows":1,"Actual Loops":1}}]`},
		{"missing counters", `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"objects","Alias":"o"}}]`},
		{"missing buffers", `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"objects","Alias":"o","Actual Rows":1,"Actual Loops":1}}]`},
		{"inconsistent buffers", `[{"Plan":{"Node Type":"Bitmap Heap Scan","Relation Name":"objects","Alias":"o","Actual Rows":1,"Actual Loops":1,"Shared Hit Blocks":0,"Shared Read Blocks":0,"Plans":[{"Node Type":"Bitmap Index Scan","Index Name":"objects_gc","Actual Rows":1,"Actual Loops":1,"Shared Hit Blocks":1,"Shared Read Blocks":0}]}}]`},
		{"negative counter", `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"objects","Alias":"o","Actual Rows":-1,"Actual Loops":1}}]`},
		{"unknown scope", `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"objects","Alias":"unexpected_alias","Actual Rows":1,"Actual Loops":1}}]`},
		{"unknown relation", `[{"Plan":{"Node Type":"Seq Scan","Relation Name":"unexpected","Alias":"x","Actual Rows":1,"Actual Loops":1}}]`},
		{"orphan bitmap", `[{"Plan":{"Node Type":"Bitmap Index Scan","Index Name":"objects_gc","Actual Rows":1,"Actual Loops":1}}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := analyzeGCPlanWork(json.RawMessage(tc.plan))
			require.Error(t, err)
		})
	}
}

type gcPlanRelationWork struct {
	Scope            string  `json:"Scope"`
	Relation         string  `json:"Relation"`
	Alias            string  `json:"Alias"`
	NodeType         string  `json:"NodeType"`
	IndexName        string  `json:"IndexName,omitempty"`
	Rows             float64 `json:"Rows"`
	Loops            float64 `json:"Loops"`
	SharedHitBlocks  float64 `json:"SharedHitBlocks"`
	SharedReadBlocks float64 `json:"SharedReadBlocks"`
}

type gcWorkPlanNode struct {
	NodeType       string           `json:"Node Type"`
	Relation       string           `json:"Relation Name"`
	Alias          string           `json:"Alias"`
	IndexName      string           `json:"Index Name"`
	SubplanName    string           `json:"Subplan Name"`
	ActualRows     *float64         `json:"Actual Rows"`
	ActualLoops    *float64         `json:"Actual Loops"`
	RemovedFilter  float64          `json:"Rows Removed by Filter"`
	RemovedRecheck float64          `json:"Rows Removed by Index Recheck"`
	SharedHits     *float64         `json:"Shared Hit Blocks"`
	SharedReads    *float64         `json:"Shared Read Blocks"`
	Plans          []gcWorkPlanNode `json:"Plans"`
}

// analyzeGCPlanWork measures work visible in EXPLAIN, not exact physical rows
// visited: PostgreSQL rounds per-loop averages and omits dead index entries and
// unsuccessful probes. Bitmap heap tuples and index entries remain separate
// observations so a selective bitmap intersection cannot hide a broad scan.
func analyzeGCPlanWork(raw json.RawMessage) ([]gcPlanRelationWork, error) {
	var statements []struct {
		Plan *gcWorkPlanNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &statements); err != nil {
		return nil, fmt.Errorf("decode GC plan: %w", err)
	}
	if len(statements) != 1 || statements[0].Plan == nil {
		return nil, fmt.Errorf("expected one GC plan with an execution root")
	}
	var work []gcPlanRelationWork
	if err := collectGCPlanWork(*statements[0].Plan, "", nil, &work); err != nil {
		return nil, err
	}
	return work, nil
}

func collectGCPlanWork(node gcWorkPlanNode, scope string, bitmap *gcPlanRelationWork, work *[]gcPlanRelationWork) error {
	if node.ActualLoops == nil || node.ActualRows == nil {
		return fmt.Errorf("GC plan %q lacks ANALYZE counters", node.NodeType)
	}
	if *node.ActualLoops < 0 || *node.ActualRows < 0 || node.RemovedFilter < 0 || node.RemovedRecheck < 0 {
		return fmt.Errorf("GC plan %q has negative work counters", node.NodeType)
	}
	if *node.ActualLoops == 0 {
		return nil
	}
	if node.SubplanName == "CTE page" || node.SubplanName == "CTE c" {
		scope = "candidate"
	}
	scan := gcPlanRelationWork{Scope: scope, Relation: node.Relation, Alias: node.Alias, NodeType: node.NodeType, IndexName: node.IndexName,
		Rows: (*node.ActualRows + node.RemovedFilter + node.RemovedRecheck) * *node.ActualLoops, Loops: *node.ActualLoops}
	switch node.NodeType {
	case "Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Tid Scan", "Tid Range Scan":
		switch node.Relation {
		case "objects":
			if node.Alias == "o" {
				scan.Scope = "target"
			} else if scope == "candidate" || node.Alias == "objects" {
				scan.Scope = "candidate"
			} else {
				return fmt.Errorf("unknown GC objects scan scope for alias %q", node.Alias)
			}
		case "hot_batches", "active_segment_blocks", "generation_blocks", "segment_generations":
			scan.Scope = "reference"
		default:
			return fmt.Errorf("unexpected GC relation %q", node.Relation)
		}
		if node.NodeType == "Bitmap Heap Scan" {
			bitmap = &scan
		}
	case "Bitmap Index Scan":
		if bitmap == nil || node.IndexName == "" {
			return fmt.Errorf("GC bitmap index scan lacks a heap relation or index name")
		}
		scan.Scope, scan.Relation, scan.Alias = bitmap.Scope, bitmap.Relation, bitmap.Alias
	case "Result", "Aggregate", "Hash", "Sort", "Incremental Sort", "Limit", "LockRows", "Materialize", "Memoize",
		"Append", "Merge Append", "Gather", "Gather Merge", "Nested Loop", "Hash Join", "Merge Join", "Unique", "SetOp",
		"Subquery Scan", "CTE Scan", "Function Scan", "Values Scan", "ProjectSet", "WindowAgg", "Recursive Union",
		"WorkTable Scan", "Group", "ModifyTable", "BitmapAnd", "BitmapOr":
		// These nodes carry no independently counted physical-relation work.
	default:
		return fmt.Errorf("unsupported GC plan node %q", node.NodeType)
	}
	if scan.Relation != "" && node.NodeType != "ModifyTable" {
		if node.SharedHits == nil || node.SharedReads == nil {
			return fmt.Errorf("GC relation scan %q lacks BUFFERS counters", node.NodeType)
		}
		scan.SharedHitBlocks, scan.SharedReadBlocks = *node.SharedHits, *node.SharedReads
		// Node buffers include child work, including bitmap construction and
		// reference subplans. Count only this node's contribution once.
		for _, child := range node.Plans {
			if child.SharedHits != nil {
				scan.SharedHitBlocks -= *child.SharedHits
			}
			if child.SharedReads != nil {
				scan.SharedReadBlocks -= *child.SharedReads
			}
		}
		if scan.SharedHitBlocks < 0 || scan.SharedReadBlocks < 0 {
			return fmt.Errorf("GC relation scan %q has inconsistent inclusive buffers", node.NodeType)
		}
		*work = append(*work, scan)
	}
	for _, child := range node.Plans {
		if err := collectGCPlanWork(child, scope, bitmap, work); err != nil {
			return err
		}
	}
	return nil
}

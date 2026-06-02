package world

import "math/rand/v2"

func initialRecordCounts(cfg Config) []int {
	out := make([]int, cfg.Accounts)
	if cfg.InitialRecords > 0 {
		for i := range out {
			out[i] = cfg.InitialRecords
		}
		return out
	}

	minN := max(cfg.InitialRecordsMin, 0)
	maxN := max(cfg.InitialRecordsMax, minN)
	r := rand.New(rand.NewPCG(cfg.Seed, 0x1a17))
	for i := range out {
		out[i] = clamp(sampleInitialRecordCount(r, maxN), minN, maxN)
	}
	return out
}

func sampleInitialRecordCount(r *rand.Rand, maxN int) int {
	if maxN <= 0 {
		return 0
	}

	p := r.Float64()
	switch {
	case p < 0.30:
		return 0
	case p < 0.70:
		return 1 + r.IntN(min(maxN, 10))
	case p < 0.95:
		return 10 + r.IntN(max(1, min(maxN, 100)-9))
	default:
		return 100 + r.IntN(max(1, maxN-99))
	}
}

func clamp(v, lo, hi int) int {
	return min(max(v, lo), hi)
}

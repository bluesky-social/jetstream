package http

import "sync"

// FaultPlan is a deterministic, test-owned simulator fault schedule.
// Methods are goroutine-safe because backfill workers may issue getRepo
// requests concurrently.
//
// A nil *FaultPlan is valid and behaves as "no faults": every method is
// nil-safe. This lets the production handler (NewHandler) pass a nil
// plan unconditionally instead of branching on whether faults are
// configured.
type FaultPlan struct {
	getRepo        *getRepoFaults
	subscribeRepos *subscribeReposFaults
}

type getRepoFaults struct {
	mu            sync.Mutex
	httpByDID     map[string]httpFaultState
	truncateByDID map[string]countFaultState
}

type httpFaultState struct {
	status    int
	remaining int
	fired     int
}

type countFaultState struct {
	remaining int
	fired     int
}

type subscribeReposFaults struct {
	mu          sync.Mutex
	schedule    []int
	nextConn    int
	connections int
	disconnects int
}

// NewFaultPlan constructs an empty fault plan.
func NewFaultPlan() *FaultPlan {
	return &FaultPlan{
		getRepo: &getRepoFaults{
			httpByDID:     make(map[string]httpFaultState),
			truncateByDID: make(map[string]countFaultState),
		},
		subscribeRepos: &subscribeReposFaults{},
	}
}

// AddGetRepoHTTPFailures schedules count HTTP failures for did before
// getRepo returns to normal simulator behavior.
func (p *FaultPlan) AddGetRepoHTTPFailures(did string, status, count int) {
	if p == nil || p.getRepo == nil || did == "" || count <= 0 {
		return
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	st := p.getRepo.httpByDID[did]
	st.status = status
	st.remaining += count
	p.getRepo.httpByDID[did] = st
}

// GetRepoHTTPFailuresFired reports how many scheduled getRepo HTTP
// failures have fired for did.
func (p *FaultPlan) GetRepoHTTPFailuresFired(did string) int {
	if p == nil || p.getRepo == nil {
		return 0
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	return p.getRepo.httpByDID[did].fired
}

// AddGetRepoCARTruncations schedules count successful-status getRepo
// responses for did whose CAR body is cut short before getRepo returns to
// normal simulator behavior.
func (p *FaultPlan) AddGetRepoCARTruncations(did string, count int) {
	if p == nil || p.getRepo == nil || did == "" || count <= 0 {
		return
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	st := p.getRepo.truncateByDID[did]
	st.remaining += count
	p.getRepo.truncateByDID[did] = st
}

// GetRepoCARTruncationsFired reports how many scheduled getRepo CAR
// truncations have fired for did.
func (p *FaultPlan) GetRepoCARTruncationsFired(did string) int {
	if p == nil || p.getRepo == nil {
		return 0
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	return p.getRepo.truncateByDID[did].fired
}

// SetSubscribeReposDisconnectSchedule installs per-connection frame
// thresholds. Each accepted subscribeRepos connection consumes at most one
// threshold; connections after the schedule is exhausted serve normally.
func (p *FaultPlan) SetSubscribeReposDisconnectSchedule(thresholds []int) {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	p.subscribeRepos.schedule = append(p.subscribeRepos.schedule[:0], thresholds...)
	p.subscribeRepos.nextConn = 0
	p.subscribeRepos.connections = 0
	p.subscribeRepos.disconnects = 0
}

// SubscribeReposConnections reports accepted subscribeRepos websocket
// connections.
func (p *FaultPlan) SubscribeReposConnections() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.connections
}

// SubscribeReposDisconnects reports how many subscribeRepos connections were
// closed by the configured fault schedule.
func (p *FaultPlan) SubscribeReposDisconnects() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.disconnects
}

func (p *FaultPlan) maybeGetRepoHTTPFault(did string) (int, bool) {
	if p == nil || p.getRepo == nil {
		return 0, false
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	st, ok := p.getRepo.httpByDID[did]
	if !ok || st.remaining <= 0 {
		return 0, false
	}
	st.remaining--
	st.fired++
	p.getRepo.httpByDID[did] = st
	return st.status, true
}

func (p *FaultPlan) maybeGetRepoCARTruncation(did string) bool {
	if p == nil || p.getRepo == nil {
		return false
	}
	p.getRepo.mu.Lock()
	defer p.getRepo.mu.Unlock()
	st, ok := p.getRepo.truncateByDID[did]
	if !ok || st.remaining <= 0 {
		return false
	}
	st.remaining--
	st.fired++
	p.getRepo.truncateByDID[did] = st
	return true
}

func (p *FaultPlan) onSubscribeConnect() (int, bool) {
	if p == nil || p.subscribeRepos == nil {
		return 0, false
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	p.subscribeRepos.connections++
	if p.subscribeRepos.nextConn >= len(p.subscribeRepos.schedule) {
		return 0, false
	}
	threshold := p.subscribeRepos.schedule[p.subscribeRepos.nextConn]
	p.subscribeRepos.nextConn++
	return threshold, threshold > 0
}

func (p *FaultPlan) noteSubscribeDisconnect() {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	p.subscribeRepos.disconnects++
	p.subscribeRepos.mu.Unlock()
}

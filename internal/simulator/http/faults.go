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

// SubscribeReposReplayFault schedules one seq-replay fault on a
// subscribeRepos connection: after AfterFrames frames have been written,
// the relay re-sends previously delivered frames verbatim — duplicate
// and regressed seqs, the wire shape of a relay restored from backup.
// Exactly one of DuplicateLast / RegressToSeq selects the mode:
//
//   - DuplicateLast > 0: re-send the last N frames this connection
//     already delivered (a short duplicate burst).
//   - RegressToSeq > 0: re-send every retained frame with seq >
//     RegressToSeq up to the tip at fire time (a whole replayed window).
//
// Re-sent frames do not count toward AfterFrames or the disconnect
// schedule's thresholds, so replay and disconnect faults compose
// deterministically on one connection.
type SubscribeReposReplayFault struct {
	AfterFrames   int
	DuplicateLast int
	RegressToSeq  int64
}

// SubscribeReposInjectFault schedules one wire-level frame fault on a
// subscribeRepos connection: after AfterFrames counted frames have been
// written, the relay injects Frame verbatim onto the wire (if non-empty)
// and then swallows the next real frame (if SwallowNext). The
// combinations model the frame-level adversity catalog:
//
//   - Frame only: a spurious extra frame between real ones — garbage
//     CBOR, an unknown frame type, an op=-1 error frame, or an
//     oversized frame, depending on the bytes.
//   - Frame + SwallowNext: in-place corruption — the next real frame
//     is replaced positionally by the injected bytes.
//   - SwallowNext only: a pure drop — the relay consumed a seq the
//     subscriber never receives, i.e. a genuine wire-level gap.
//
// Injected bytes bypass the write accounting entirely: they do not
// count toward AfterFrames, the disconnect schedule's thresholds, or a
// replay fault's duplicate ring. A swallowed frame is likewise not
// counted (it never reaches the wire). The world's pebble-backed
// firehose history keeps the true frames either way — the fault is
// wire-only, so a reconnecting client can still recover the real
// stream.
//
// The fault fires after the AfterFrames-th counted frame is written;
// AfterFrames <= 1 therefore fires after the connection's first frame,
// never before it (mirroring SubscribeReposReplayFault).
type SubscribeReposInjectFault struct {
	AfterFrames int
	Frame       []byte
	SwallowNext bool
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

	replaySchedule []SubscribeReposReplayFault
	replayNextConn int
	replaysFired   int
	// replayedFrames is the total count of frames re-sent by fired
	// replay faults, across all connections. The oracle uses it to
	// bound storage bloat: re-archived rows can never exceed the rows
	// that were re-delivered.
	replayedFrames int

	injectSchedule []SubscribeReposInjectFault
	injectNextConn int
	injectsFired   int
	// swallowedFrames counts real frames suppressed by fired inject
	// faults (SwallowNext). The oracle uses it for exact loss
	// accounting: the archive must be missing exactly the swallowed
	// frames' rows, no more.
	swallowedFrames int
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

// SetSubscribeReposReplaySchedule installs per-connection seq-replay
// faults. Each accepted subscribeRepos connection consumes at most one
// entry; connections after the schedule is exhausted serve normally. A
// zero-valued entry (neither DuplicateLast nor RegressToSeq set) arms
// nothing for that connection.
func (p *FaultPlan) SetSubscribeReposReplaySchedule(faults []SubscribeReposReplayFault) {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	p.subscribeRepos.replaySchedule = append(p.subscribeRepos.replaySchedule[:0], faults...)
	p.subscribeRepos.replayNextConn = 0
	p.subscribeRepos.replaysFired = 0
	p.subscribeRepos.replayedFrames = 0
}

// SubscribeReposReplaysFired reports how many scheduled seq-replay faults
// have fired.
func (p *FaultPlan) SubscribeReposReplaysFired() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.replaysFired
}

// SubscribeReposReplayedFrames reports the total number of frames re-sent
// by fired seq-replay faults. This is the oracle's storage-bloat bound:
// duplicate rows in the archive cannot exceed the rows expanded from
// these re-delivered frames.
func (p *FaultPlan) SubscribeReposReplayedFrames() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.replayedFrames
}

func (p *FaultPlan) onSubscribeConnectReplay() (SubscribeReposReplayFault, bool) {
	if p == nil || p.subscribeRepos == nil {
		return SubscribeReposReplayFault{}, false
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	if p.subscribeRepos.replayNextConn >= len(p.subscribeRepos.replaySchedule) {
		return SubscribeReposReplayFault{}, false
	}
	f := p.subscribeRepos.replaySchedule[p.subscribeRepos.replayNextConn]
	p.subscribeRepos.replayNextConn++
	armed := f.DuplicateLast > 0 || f.RegressToSeq > 0
	return f, armed
}

func (p *FaultPlan) noteSubscribeReplay(frames int) {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	p.subscribeRepos.replaysFired++
	p.subscribeRepos.replayedFrames += frames
	p.subscribeRepos.mu.Unlock()
}

// SetSubscribeReposInjectSchedule installs per-connection frame-inject
// faults. Each accepted subscribeRepos connection consumes at most one
// entry; connections after the schedule is exhausted serve normally. A
// zero-valued entry (no Frame, no SwallowNext) arms nothing for that
// connection.
func (p *FaultPlan) SetSubscribeReposInjectSchedule(faults []SubscribeReposInjectFault) {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	p.subscribeRepos.injectSchedule = append(p.subscribeRepos.injectSchedule[:0], faults...)
	p.subscribeRepos.injectNextConn = 0
	p.subscribeRepos.injectsFired = 0
	p.subscribeRepos.swallowedFrames = 0
}

// SubscribeReposInjectsFired reports how many scheduled frame-inject
// faults have fired.
func (p *FaultPlan) SubscribeReposInjectsFired() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.injectsFired
}

// SubscribeReposSwallowedFrames reports how many real frames fired
// inject faults suppressed from the wire. This is the oracle's exact
// loss bound: the archive must be missing precisely these frames' rows.
func (p *FaultPlan) SubscribeReposSwallowedFrames() int {
	if p == nil || p.subscribeRepos == nil {
		return 0
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	return p.subscribeRepos.swallowedFrames
}

func (p *FaultPlan) onSubscribeConnectInject() (SubscribeReposInjectFault, bool) {
	if p == nil || p.subscribeRepos == nil {
		return SubscribeReposInjectFault{}, false
	}
	p.subscribeRepos.mu.Lock()
	defer p.subscribeRepos.mu.Unlock()
	if p.subscribeRepos.injectNextConn >= len(p.subscribeRepos.injectSchedule) {
		return SubscribeReposInjectFault{}, false
	}
	f := p.subscribeRepos.injectSchedule[p.subscribeRepos.injectNextConn]
	p.subscribeRepos.injectNextConn++
	armed := len(f.Frame) > 0 || f.SwallowNext
	return f, armed
}

func (p *FaultPlan) noteSubscribeInject() {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	p.subscribeRepos.injectsFired++
	p.subscribeRepos.mu.Unlock()
}

func (p *FaultPlan) noteSubscribeSwallow() {
	if p == nil || p.subscribeRepos == nil {
		return
	}
	p.subscribeRepos.mu.Lock()
	p.subscribeRepos.swallowedFrames++
	p.subscribeRepos.mu.Unlock()
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

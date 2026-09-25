// Package leader runs Jetstream's writer election loop (design §6.3).
//
// A process runs at most one writer session at a time. Run acquires the
// lock, hands the session its writer epoch, renews the lease in the
// background, and cancels the session when the lease is lost. When a session
// ends it releases the lock and either starts over or, for a fatal error,
// returns so the process can exit non-zero.
//
// Lease timing only affects how fast failover happens. Safety comes from the
// epoch fence on every leader write (design §6.4), not from this loop: a
// paused leader can keep running after its lease is gone, and the fence
// rejects its writes.
//
// Local mode uses Local, which always holds epoch 1 and never renews.
package leader

package web

import (
	"sync"
	"time"
)

const repoActionLimiterMaxEntries = 4096

var defaultRepoActionRateLimit = RateLimit{
	Limit:  4,
	Window: time.Minute,
}

// RateLimit configures a fixed-window per-source-IP limiter.
type RateLimit struct {
	Limit  int
	Window time.Duration
}

type repoActionLimiter struct {
	mu      sync.Mutex
	now     func() time.Time
	limit   int
	window  time.Duration
	entries map[string]repoActionBucket
}

type repoActionBucket struct {
	windowStart time.Time
	lastSeen    time.Time
	used        int
}

func newRepoActionLimiter(cfg RateLimit, now func() time.Time) *repoActionLimiter {
	return &repoActionLimiter{
		now:     now,
		limit:   cfg.Limit,
		window:  cfg.Window,
		entries: make(map[string]repoActionBucket),
	}
}

func (l *repoActionLimiter) allow(ip string) bool {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) >= repoActionLimiterMaxEntries {
		l.pruneLocked(now)
	}
	if len(l.entries) >= repoActionLimiterMaxEntries {
		l.evictOldestLocked()
	}

	b := l.entries[ip]
	if b.windowStart.IsZero() || now.Sub(b.windowStart) >= l.window {
		b.windowStart = now
		b.used = 0
	}
	b.lastSeen = now
	if b.used >= l.limit {
		l.entries[ip] = b
		return false
	}
	b.used++
	l.entries[ip] = b
	return true
}

func (l *repoActionLimiter) pruneLocked(now time.Time) {
	for ip, b := range l.entries {
		if now.Sub(b.lastSeen) >= 2*l.window {
			delete(l.entries, ip)
		}
	}
}

func (l *repoActionLimiter) evictOldestLocked() {
	var oldestIP string
	var oldest time.Time
	for ip, b := range l.entries {
		if oldestIP == "" || b.lastSeen.Before(oldest) {
			oldestIP = ip
			oldest = b.lastSeen
		}
	}
	delete(l.entries, oldestIP)
}

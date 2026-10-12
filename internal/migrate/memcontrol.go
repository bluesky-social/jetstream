package migrate

import (
	"context"
	"slices"
	"sync"
)

// MemControl is an in-memory Control for tests and for a migrator with no
// control table.
type MemControl struct {
	mu     sync.Mutex
	next   int64
	reqs   []memRequest
	status []byte
}

type memRequest struct {
	Request
	claimed, acked bool
	result         string
}

// Request records an operator request and returns its ID.
func (c *MemControl) Request(action string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.next++
	c.reqs = append(c.reqs, memRequest{Request: Request{ID: c.next, Action: action}})
	return c.next
}

// Result returns a request's answer, once there is one, and whether a
// migrator claimed it.
func (c *MemControl) Result(id int64) (result string, answered, claimed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.reqs {
		if r.ID == id {
			return r.result, r.acked, r.claimed
		}
	}
	return "", false, false
}

// Withdraw answers a request no migrator has claimed, and reports whether
// it did.
func (c *MemControl) Withdraw(id int64, result string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.reqs {
		if r := &c.reqs[i]; r.ID == id && !r.claimed && !r.acked {
			r.acked, r.result = true, result
			return true
		}
	}
	return false
}

// LastStatus returns the last status document published.
func (c *MemControl) LastStatus() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.status)
}

// Claim implements Control.
func (c *MemControl) Claim(context.Context) (Request, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.reqs {
		if r := &c.reqs[i]; !r.claimed && !r.acked {
			r.claimed = true
			return r.Request, true, nil
		}
	}
	return Request{}, false, nil
}

// Ack implements Control.
func (c *MemControl) Ack(_ context.Context, id int64, result string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.reqs {
		if r := &c.reqs[i]; r.ID == id && !r.acked {
			r.acked, r.result = true, result
		}
	}
	return nil
}

// AbandonAll implements Control.
func (c *MemControl) AbandonAll(_ context.Context, result string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.reqs {
		if r := &c.reqs[i]; !r.acked {
			r.acked, r.result = true, result
		}
	}
	return nil
}

// SetStatus implements Control.
func (c *MemControl) SetStatus(_ context.Context, status []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status = slices.Clone(status)
	return nil
}

package leader_test

import (
	"testing"
	"time"

	"github.com/bluesky-social/jetstream/internal/leader"
	"github.com/bluesky-social/jetstream/internal/leader/lockertest"
)

func TestLocalLockerContract(t *testing.T) {
	t.Parallel()
	lockertest.Run(t, func(t *testing.T) lockertest.Backend {
		return lockertest.Backend{
			NewLocker: func() leader.Locker { return leader.Local{} },
			Advance:   func(time.Duration) {},
			Lease:     time.Second,
		}
	})
}

package segment

import (
	"fmt"
	"math"
)

// validate checks that ev's fields fit the on-disk column widths and
// that Kind is in range. It performs no I/O and never panics.
func validate(ev Event) error {
	if ev.Kind < KindCreate || ev.Kind > KindSync {
		return fmt.Errorf("%w: %d", ErrInvalidKind, ev.Kind)
	}
	if len(ev.DID) > math.MaxUint16 {
		return fmt.Errorf("%w: did is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.DID), math.MaxUint16)
	}
	if len(ev.Collection) > math.MaxUint8 {
		return fmt.Errorf("%w: collection is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Collection), math.MaxUint8)
	}
	if len(ev.Rkey) > math.MaxUint8 {
		return fmt.Errorf("%w: rkey is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rkey), math.MaxUint8)
	}
	if len(ev.Rev) > math.MaxUint8 {
		return fmt.Errorf("%w: rev is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Rev), math.MaxUint8)
	}
	if len(ev.Payload) > math.MaxUint32 {
		return fmt.Errorf("%w: payload is %d bytes (max %d)",
			ErrFieldTooLong, len(ev.Payload), math.MaxUint32)
	}
	return nil
}

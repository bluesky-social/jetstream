package orchestrator

import "errors"

// ErrInvalidConfig is returned by Config.validate when a required
// field is missing. Wrapped with a field-naming context so callers
// see which field is at fault.
var ErrInvalidConfig = errors.New("orchestrator: invalid config")

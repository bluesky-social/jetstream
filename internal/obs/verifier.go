package obs

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// VerifierMetrics owns prometheus state for the firehose verifier.
// A nil *VerifierMetrics is a valid zero-value: every method is a
// no-op so tests can skip metric registration entirely.
type VerifierMetrics struct {
	Failures *prometheus.CounterVec
}

// NewVerifierMetrics registers verifier counters against reg.
func NewVerifierMetrics(reg prometheus.Registerer) *VerifierMetrics {
	m := &VerifierMetrics{
		Failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "verifier",
			Name:      "failures_total",
			Help:      "Number of verifier rejections, by error class. kind=other is the catch-all and should be near-zero in steady state; sustained kind=other is a signal that a new error class needs categorizing.",
		}, []string{"kind"}),
	}
	reg.MustRegister(m.Failures)
	return m
}

// IncFailure increments the counter for the given kind. nil-safe.
func (m *VerifierMetrics) IncFailure(kind string) {
	if m != nil {
		m.Failures.WithLabelValues(kind).Inc()
	}
}

// Classify maps a verifier error to one of a small closed enum of
// kinds. Unknown errors map to "other" — bounded cardinality matters
// more than per-error specificity; kind="other" being non-zero is
// itself an operator signal.
//
// Classification is a substring match on the error message rather
// than errors.Is/errors.As because atmos's verifier surfaces many
// errors as fmt.Errorf strings without sentinel types. If atmos
// later exports sentinel errors, switch this to errors.Is — the
// signature is unchanged.
func Classify(err error) string {
	if err == nil {
		return "other"
	}
	msg := strings.ToLower(err.Error())
	// Order encodes precedence: signature beats chain beats hosting
	// beats resolve. A signature failure inside a chain walk is still
	// a signature failure; a hosting check inside a resolve flow is
	// still a hosting failure.
	switch {
	case strings.Contains(msg, "signature"):
		return "signature"
	case strings.Contains(msg, "chain"):
		return "chain"
	case strings.Contains(msg, "hosting"):
		return "hosting"
	case strings.Contains(msg, "resolve"):
		return "resolve"
	default:
		return "other"
	}
}

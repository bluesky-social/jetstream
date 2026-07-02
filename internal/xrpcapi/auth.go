package xrpcapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/jcalabro/atmos/xrpcserver"
)

// withBearer wraps h so it only runs when the request carries the exact
// configured bearer token. It is jetstream's first authenticated surface
// (design Q-JOB): the timestamp-import endpoints modify the archive, so they
// are admin-only.
//
// Secure by default: when token is empty (no --timestamp-import-token
// configured) EVERY request is rejected with 401, and the response does not
// distinguish "disabled" from "wrong token" so a probe cannot learn whether
// import is enabled. The comparison is constant-time (crypto/subtle) so a
// timing side channel cannot recover the token byte by byte.
//
// TLS is intentionally NOT enforced in-process: jetstream serves plain HTTP on
// a bare listener with TLS terminated at an upstream proxy, so an r.TLS check
// would be theater. The operator is responsible for fronting the endpoint with
// TLS (documented in the operator notes).
func withBearer(token string, h xrpcserver.Handler) xrpcserver.Handler {
	// Compare sha256 digests, not the raw bytes: ConstantTimeCompare returns
	// immediately on a length mismatch, so a raw compare would leak the
	// configured token's length. Fixed-width digests keep every rejection —
	// disabled, missing header, wrong token — on the same code path with the
	// same body, so neither the response nor its timing reveals which case hit.
	tokenHash := sha256.Sum256([]byte(token))
	enabled := len(token) > 0
	return xrpcserver.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
		presented, ok := bearerToken(r.HTTPReq)
		presentedHash := sha256.Sum256([]byte(presented))
		match := subtle.ConstantTimeCompare(presentedHash[:], tokenHash[:]) == 1
		// enabled is checked in the same branch (not early-returned): when the
		// token is empty a presented empty string would hash-match, and the
		// disabled case must reject everything.
		if !enabled || !ok || !match {
			return xrpcserver.AuthRequired("invalid or missing bearer token")
		}
		return h.ServeXRPC(ctx, w, r)
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header. ok is false when the header is absent or not a Bearer scheme.
func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	return h[len(prefix):], true
}

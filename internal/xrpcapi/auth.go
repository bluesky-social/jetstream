package xrpcapi

import (
	"context"
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
	tokenBytes := []byte(token)
	return xrpcserver.HandlerFunc(func(ctx context.Context, w http.ResponseWriter, r *xrpcserver.Request) error {
		if len(tokenBytes) == 0 {
			return xrpcserver.AuthRequired("timestamp import is not enabled")
		}
		presented, ok := bearerToken(r.HTTPReq)
		// Always run the constant-time compare, even on a missing header, so
		// the auth path's timing does not reveal whether a token was presented.
		if subtle.ConstantTimeCompare([]byte(presented), tokenBytes) != 1 || !ok {
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

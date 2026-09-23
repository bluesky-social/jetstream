package xrpcapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/jcalabro/atmos/xrpcserver"
)

// withBearer requires the configured timestamp-import bearer token. An empty
// token disables access; disabled, missing, and incorrect credentials all
// return the same 401 response. Token digests are compared in constant time.
//
// The operator must terminate TLS at the upstream proxy. Jetstream's listener
// receives plain HTTP and does not enforce TLS here.
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

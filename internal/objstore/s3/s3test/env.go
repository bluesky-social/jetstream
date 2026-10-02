// Package s3test holds test helpers for the S3 Blob. Env points a test at a
// real object store from JETSTREAM_TEST_S3_* variables (the `just
// test-storage` path). Fake serves S3 in memory behind an http.RoundTripper,
// so the Blob's HTTP handling runs in `just` with no containers.
// FaultTransport injects the failures a real network produces.
//
// It never imports the AWS SDK: everything here is plain net/http, so the
// driver import boundary stays at objstore/s3.
package s3test

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/bluesky-social/jetstream/internal/objstore/s3"
)

// Environment variables that select a real object store for tests.
const (
	EnvEndpoint        = "JETSTREAM_TEST_S3_ENDPOINT"
	EnvRegion          = "JETSTREAM_TEST_S3_REGION"
	EnvBucket          = "JETSTREAM_TEST_S3_BUCKET"
	EnvAccessKeyID     = "JETSTREAM_TEST_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey = "JETSTREAM_TEST_S3_SECRET_ACCESS_KEY"
	// EnvStorageRequired set to 1 turns a missing configuration into a
	// failure, so `just test-storage` cannot pass by skipping everything.
	EnvStorageRequired = "JETSTREAM_TEST_STORAGE_REQUIRED"
)

// Env returns an s3.Config for the object store the environment names. It
// uses path-style addressing (MinIO and SeaweedFS need it) and a random
// per-call key prefix, so parallel tests and repeated runs against one
// bucket never see each other's keys. The credentials lack ListBucket in
// `just up`, so nothing here cleans up by listing; the bucket is ephemeral.
//
// When JETSTREAM_TEST_S3_ENDPOINT is unset, Env skips t, or fails it if
// JETSTREAM_TEST_STORAGE_REQUIRED=1.
func Env(t testing.TB) s3.Config {
	t.Helper()
	endpoint := os.Getenv(EnvEndpoint)
	if endpoint == "" {
		if os.Getenv(EnvStorageRequired) == "1" {
			t.Fatalf("%s=1 but %s is unset", EnvStorageRequired, EnvEndpoint)
		}
		t.Skipf("%s unset; real object store tests run under `just test-storage`", EnvEndpoint)
	}
	bucket := os.Getenv(EnvBucket)
	if bucket == "" {
		t.Fatalf("%s is set but %s is not", EnvEndpoint, EnvBucket)
	}
	region := os.Getenv(EnvRegion)
	if region == "" {
		region = "us-east-1"
	}
	return s3.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		Prefix:          RandomPrefix(t),
		PathStyle:       true,
		AccessKeyID:     os.Getenv(EnvAccessKeyID),
		SecretAccessKey: os.Getenv(EnvSecretAccessKey),
	}
}

// RandomPrefix returns a fresh key prefix under "jetstream-test/".
func RandomPrefix(t testing.TB) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "jetstream-test/" + hex.EncodeToString(b[:])
}

// New returns a Blob for cfg and fails t on error.
func New(t testing.TB, cfg s3.Config) *s3.Blob {
	t.Helper()
	b, err := s3.New(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

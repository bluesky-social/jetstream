// Package s3 is the objstore.Blob for S3-compatible object stores (AWS S3,
// MinIO, SeaweedFS) on aws-sdk-go-v2. It is the only package allowed to
// import the AWS SDK (TestStorageDriverImportBoundary).
//
// The Blob owns transport concerns only: key prefixing, concurrency limits,
// retries with backoff, error mapping, and the jetstream_s3_* metrics. It
// never verifies object bytes. objstore/protocol does that against the
// catalog.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/bluesky-social/jetstream/internal/objstore"
	"github.com/bluesky-social/jetstream/internal/obs"
)

// Defaults from design §18.
const (
	DefaultUploadConcurrency = 8
	DefaultReadConcurrency   = 32
	DefaultRetryTimeout      = 30 * time.Second
	DefaultAttemptTimeout    = 10 * time.Second
)

// Config configures a Blob. Credentials never appear in logs, errors, or
// spans.
type Config struct {
	// Endpoint is the S3 endpoint URL (JETSTREAM_S3_ENDPOINT). Empty uses
	// AWS's own endpoint resolution for Region.
	Endpoint string
	Region   string
	Bucket   string
	// Prefix goes in front of every key, joined with "/"
	// (JETSTREAM_S3_PREFIX). Empty means keys sit at the bucket root.
	Prefix string
	// PathStyle addresses the bucket in the URL path rather than the host
	// name. MinIO and SeaweedFS need it.
	PathStyle bool

	// UploadConcurrency bounds concurrent PUTs and DELETEs process-wide
	// (JETSTREAM_S3_UPLOAD_CONCURRENCY). Zero means the default.
	UploadConcurrency int
	// ReadConcurrency bounds concurrent GETs process-wide
	// (JETSTREAM_S3_READ_CONCURRENCY). Zero means the default.
	ReadConcurrency int
	// RetryTimeout bounds one call's attempts and backoff waits together
	// (JETSTREAM_S3_RETRY_TIMEOUT). After it the call fails and the caller
	// decides what happens (design §7.3). Zero means the default.
	RetryTimeout time.Duration
	// AttemptTimeout bounds one HTTP attempt, so a hung connection turns
	// into a retry instead of consuming the whole RetryTimeout. Zero means
	// the default.
	AttemptTimeout time.Duration

	// AccessKeyID and SecretAccessKey select static credentials. When both
	// are empty the AWS SDK default chain is used (environment, shared
	// config, web identity, instance metadata).
	AccessKeyID     string
	SecretAccessKey string

	// Transport, when set, carries every HTTP request. Tests use it to
	// inject faults (s3test.FaultTransport) or serve S3 in memory
	// (s3test.Fake).
	Transport http.RoundTripper

	// Metrics may be nil.
	Metrics *Metrics
}

// String describes the config with the credentials redacted, so a stray %v
// or slog attribute cannot leak them (ground rule 7).
func (c Config) String() string {
	return fmt.Sprintf("s3.Config{Endpoint:%q Region:%q Bucket:%q Prefix:%q PathStyle:%v StaticCredentials:%v}",
		c.Endpoint, c.Region, c.Bucket, c.Prefix, c.PathStyle, c.AccessKeyID != "")
}

// GoString redacts %#v the same way.
func (c Config) GoString() string { return c.String() }

// LogValue implements slog.LogValuer with the credentials redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// Blob is an objstore.Blob over S3. It is safe for concurrent use.
type Blob struct {
	client  *s3.Client
	bucket  string
	prefix  string
	metrics *Metrics
	tracer  trace.Tracer

	uploads chan struct{}
	reads   chan struct{}

	retryTimeout   time.Duration
	attemptTimeout time.Duration
	backoffBase    time.Duration
	backoffMax     time.Duration
}

var _ objstore.Blob = (*Blob)(nil)

// New returns a Blob. It makes no requests: a bad endpoint or bucket shows
// up on the first call.
func New(ctx context.Context, cfg Config) (*Blob, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if cfg.Region == "" {
		return nil, errors.New("s3: region is required")
	}
	if (cfg.AccessKeyID == "") != (cfg.SecretAccessKey == "") {
		return nil, errors.New("s3: set both the access key ID and the secret access key, or neither")
	}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
		// Blob.retry owns retries so RetryTimeout, metrics, and the
		// not-found mapping see every attempt.
		config.WithRetryer(func() aws.Retryer { return aws.NopRetryer{} }),
		// MinIO and SeaweedFS lag on the SDK's default trailing checksums,
		// and objstore/protocol verifies SHA-256 end to end anyway.
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}
	if cfg.Transport != nil {
		opts = append(opts, config.WithHTTPClient(&http.Client{Transport: cfg.Transport}))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load AWS config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	b := &Blob{
		client:         client,
		bucket:         cfg.Bucket,
		prefix:         strings.Trim(cfg.Prefix, "/"),
		metrics:        cfg.Metrics,
		tracer:         obs.Tracer("objstore/s3"),
		uploads:        make(chan struct{}, orDefault(cfg.UploadConcurrency, DefaultUploadConcurrency)),
		reads:          make(chan struct{}, orDefault(cfg.ReadConcurrency, DefaultReadConcurrency)),
		retryTimeout:   orDefault(cfg.RetryTimeout, DefaultRetryTimeout),
		attemptTimeout: orDefault(cfg.AttemptTimeout, DefaultAttemptTimeout),
		backoffBase:    50 * time.Millisecond,
		backoffMax:     2 * time.Second,
	}
	return b, nil
}

func orDefault[T int | time.Duration](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

func (b *Blob) fullKey(key string) string {
	if b.prefix == "" {
		return key
	}
	return b.prefix + "/" + key
}

// PutKey implements objstore.Blob.
func (b *Blob) PutKey(ctx context.Context, key string, data []byte) error {
	_, err := call(ctx, b, b.uploads, opPut, key, func(ctx context.Context, k string) (struct{}, int, error) {
		_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        &b.bucket,
			Key:           &k,
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(len(data))),
		})
		return struct{}{}, len(data), err
	})
	return err
}

// GetKey implements objstore.Blob.
func (b *Blob) GetKey(ctx context.Context, key string) ([]byte, error) {
	return call(ctx, b, b.reads, opGet, key, func(ctx context.Context, k string) ([]byte, int, error) {
		out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &k})
		if err != nil {
			return nil, 0, err
		}
		data, err := readBody(out.Body, out.ContentLength)
		return data, len(data), err
	})
}

// GetKeyRange implements objstore.Blob.
func (b *Blob) GetKeyRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if off < 0 || n <= 0 {
		// Rejected locally: S3 has no syntax for these, and objstore.RangeLen
		// calls them ErrInvalidRange.
		return nil, fmt.Errorf("s3: get range %q: %w: off=%d n=%d", key, objstore.ErrInvalidRange, off, n)
	}
	last := int64(math.MaxInt64)
	if n <= math.MaxInt64-off {
		last = off + n - 1
	}
	rng := fmt.Sprintf("bytes=%d-%d", off, last)
	return call(ctx, b, b.reads, opGetRange, key, func(ctx context.Context, k string) ([]byte, int, error) {
		out, err := b.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &b.bucket, Key: &k, Range: &rng})
		if err != nil {
			return nil, 0, err
		}
		data, err := readBody(out.Body, out.ContentLength)
		if err == nil && int64(len(data)) > n {
			// A backend that ignored the Range header sent the whole object.
			err = fmt.Errorf("%w: asked for %d bytes, got %d", errRangeIgnored, n, len(data))
		}
		return data, len(data), err
	})
}

// DeleteKey implements objstore.Blob. S3 answers 204 for a missing key, and
// a not-found answer from any backend also counts as success.
func (b *Blob) DeleteKey(ctx context.Context, key string) error {
	_, err := call(ctx, b, b.uploads, opDelete, key, func(ctx context.Context, k string) (struct{}, int, error) {
		_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &b.bucket, Key: &k})
		return struct{}{}, 0, err
	})
	if errors.Is(err, objstore.ErrNotFound) {
		return nil
	}
	return err
}

// errRangeIgnored is not retried: the backend will keep ignoring the header.
var errRangeIgnored = errors.New("s3: range request answered with more bytes than asked for")

// readBody reads a response body and checks it against Content-Length. The
// SDK's own transport checks this, but a custom RoundTripper (or a proxy
// that breaks framing) can hand back a short body without an error, and a
// short read must be retried, not passed up as the object.
func readBody(body io.ReadCloser, contentLength *int64) ([]byte, error) {
	defer func() { _ = body.Close() }()
	var buf bytes.Buffer
	if contentLength != nil && *contentLength > 0 && *contentLength < math.MaxInt32 {
		buf.Grow(int(*contentLength))
	}
	if _, err := buf.ReadFrom(body); err != nil {
		return nil, err
	}
	if contentLength != nil && int64(buf.Len()) != *contentLength {
		return nil, fmt.Errorf("s3: body has %d bytes, Content-Length %d: %w", buf.Len(), *contentLength, io.ErrUnexpectedEOF)
	}
	return buf.Bytes(), nil
}

// call runs one Blob operation: take a concurrency slot, then attempt fn
// until it succeeds, fails permanently, or RetryTimeout runs out. It opens
// one span per call; the metrics count every attempt.
func call[T any](ctx context.Context, b *Blob, sem chan struct{}, op, key string, fn func(ctx context.Context, fullKey string) (T, int, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	fullKey := b.fullKey(key)
	ctx, span := b.tracer.Start(ctx, "s3."+op, trace.WithAttributes(
		attribute.String("s3.op", op),
		attribute.String("s3.key", fullKey),
	))
	defer span.End()

	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		span.SetStatus(codes.Error, "canceled waiting for a concurrency slot")
		return zero, ctx.Err()
	}

	start := time.Now()
	deadline := start.Add(b.retryTimeout)
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithDeadline(ctx, earliest(deadline, time.Now().Add(b.attemptTimeout)))
		t0 := time.Now()
		v, n, err := fn(actx, fullKey)
		cancel()
		err = classify(op, err)
		b.metrics.observe(op, result(ctx, err), time.Since(t0), n)
		if err == nil {
			span.SetAttributes(attribute.Int("s3.attempts", attempt), attribute.Int("s3.bytes", n))
			return v, nil
		}
		if !retryable(ctx, err) {
			return zero, finish(ctx, span, attempt, fmt.Errorf("s3: %s %q: %w", op, fullKey, err))
		}
		wait := b.backoff(attempt)
		if time.Now().Add(wait).After(deadline) {
			return zero, finish(ctx, span, attempt, fmt.Errorf("s3: %s %q: gave up after %d attempts in %v: %w",
				op, fullKey, attempt, time.Since(start).Round(time.Millisecond), err))
		}
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return zero, finish(ctx, span, attempt, fmt.Errorf("s3: %s %q: %w (last attempt: %w)", op, fullKey, ctx.Err(), err))
		}
	}
}

func finish(ctx context.Context, span trace.Span, attempts int, err error) error {
	if cerr := ctx.Err(); cerr != nil && !errors.Is(err, cerr) {
		err = fmt.Errorf("%w: %w", cerr, err)
	}
	span.SetAttributes(attribute.Int("s3.attempts", attempts))
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return err
}

func earliest(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// backoff is full-jitter exponential backoff: a uniform wait in
// [0, min(max, base*2^(attempt-1))].
func (b *Blob) backoff(attempt int) time.Duration {
	ceil := b.backoffMax
	if attempt < 30 {
		ceil = min(b.backoffMax, b.backoffBase<<(attempt-1))
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

// httpStatus and apiCode are implemented by the SDK's response and API
// errors. Matching the methods keeps smithy-go out of the direct imports.
type (
	httpStatus interface{ HTTPStatusCode() int }
	apiCode    interface{ ErrorCode() string }
)

// statusOf returns the HTTP status behind err, or 0 when there was no
// response.
func statusOf(err error) int {
	var hs httpStatus
	if errors.As(err, &hs) {
		return hs.HTTPStatusCode()
	}
	return 0
}

func codeOf(err error) string {
	var ae apiCode
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// classify maps SDK errors onto the objstore sentinels.
//
// Missing objects: 404 (MinIO, SeaweedFS, and AWS with ListBucket), and 403
// AccessDenied, which AWS returns for a missing key when the credentials
// lack ListBucket (AGENTS.md). Both become ErrNotFound, which means "maybe
// missing"; objstore/protocol decides from catalog state. A 403 with any
// other code (bad key, bad signature, expired token, skewed clock) and a 404
// NoSuchBucket are configuration problems, not missing objects, and stay
// plain errors so they cannot be read as corruption.
//
// The maybe-missing mapping applies to reads only. A 403 on a PUT or DELETE
// is a denial, and PUT never reports a missing key.
func classify(op string, err error) error {
	if err == nil {
		return nil
	}
	status, code := statusOf(err), codeOf(err)
	read := op == opGet || op == opGetRange
	switch {
	case code == "NoSuchBucket":
		return err
	case op != opPut && (status == http.StatusNotFound || code == "NoSuchKey" || code == "NotFound"):
		return fmt.Errorf("%w: %w", objstore.ErrNotFound, err)
	case read && status == http.StatusForbidden && (code == "AccessDenied" || code == ""):
		// Only a read's 403 can mean a missing key. On a delete it is a
		// real denial, and DeleteKey must not report it as done.
		return fmt.Errorf("%w (403, maybe missing): %w", objstore.ErrNotFound, err)
	case status == http.StatusRequestedRangeNotSatisfiable, code == "InvalidRange":
		return fmt.Errorf("%w: %w", objstore.ErrInvalidRange, err)
	}
	return err
}

// retryable reports whether another attempt could succeed. Caller
// cancellation, answers about the object itself, and client errors that
// will not change are final. Everything else (network errors, attempt
// timeouts, short bodies, 5xx, throttling) is retried until RetryTimeout.
func retryable(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if errors.Is(err, objstore.ErrNotFound) || errors.Is(err, objstore.ErrInvalidRange) || errors.Is(err, errRangeIgnored) {
		return false
	}
	switch s := statusOf(err); {
	case s == http.StatusRequestTimeout, s == http.StatusTooManyRequests:
		return true
	case s == http.StatusForbidden:
		// Expired or rotated credentials can recover on the SDK's next
		// credential refresh.
		return true
	case s >= 400 && s < 500:
		return false
	}
	return true
}

func result(ctx context.Context, err error) string {
	switch {
	case err == nil:
		return resultOK
	case errors.Is(err, objstore.ErrNotFound):
		return resultNotFound
	case errors.Is(err, objstore.ErrInvalidRange):
		return resultInvalidRange
	case ctx.Err() != nil:
		return resultCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return resultTimeout
	}
	return resultError
}

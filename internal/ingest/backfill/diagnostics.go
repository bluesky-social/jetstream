package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/bluesky-social/jetstream/internal/store"
	"github.com/cockroachdb/pebble"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/xrpc"
)

const (
	handleKeyPrefix = "handle/"
	hostKeyPrefix   = "host/"

	HostBucketUnresolved = "unresolved.did"
	HostBucketInvalidPDS = "invalid-pds"

	maxHostErrorSamples = 5
	maxStoredErrorBytes = 1024
)

var (
	http429ErrorRE = regexp.MustCompile(`(?i)\bhttp\s+429\b`)
	http5xxErrorRE = regexp.MustCompile(`(?i)\bhttp\s+5[0-9][0-9]\b`)

	// repoNotFoundMessageRE matches the "repo not found" message used by
	// upstreams that report a generic "NotFound" error name instead of
	// the canonical RepoNotFound lexicon error. The message is the only
	// thing distinguishing it from other generic NotFound responses
	// (e.g. a missing record or blob), so we gate on it.
	repoNotFoundMessageRE = regexp.MustCompile(`(?i)\brepo not found\b`)
)

func normalizeHandleIndexKey(handle string) ([]byte, bool) {
	normalized := strings.ToLower(strings.TrimSpace(handle))
	if normalized == "" {
		return nil, false
	}
	return []byte(handleKeyPrefix + normalized), true
}

// IdentityResolution is the durable subset of DID/handle/PDS
// resolution state maintained on RepoStatus rows.
type IdentityResolution struct {
	Handle string
	PDS    string
	Host   string
}

func normalizeHostStatusKey(host string) (string, []byte, error) {
	normalized := strings.ToLower(strings.TrimSpace(host))
	if normalized == "" {
		return "", nil, fmt.Errorf("backfill: empty host status bucket")
	}
	return normalized, []byte(hostKeyPrefix + normalized), nil
}

func stageHandleIndexSet(batch *pebble.Batch, handle string, did atmos.DID) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	if err := batch.Set(key, []byte(did), nil); err != nil {
		return fmt.Errorf("backfill: stage handle index %q: %w", handle, err)
	}
	return nil
}

func stageHandleIndexDeleteIfMatches(db *store.Store, batch *pebble.Batch, handle string, did atmos.DID) error {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return nil
	}
	val, closer, err := db.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("backfill: read handle index %q before delete: %w", handle, err)
	}
	defer func() { _ = closer.Close() }()
	if string(val) != string(did) {
		return nil
	}
	if err := batch.Delete(key, nil); err != nil {
		return fmt.Errorf("backfill: stage delete handle index %q: %w", handle, err)
	}
	return nil
}

func lookupDIDByHandle(db *store.Store, handle string) (atmos.DID, bool, error) {
	key, ok := normalizeHandleIndexKey(handle)
	if !ok {
		return "", false, nil
	}
	val, closer, err := db.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("backfill: lookup handle index %q: %w", handle, err)
	}
	defer func() { _ = closer.Close() }()
	did, err := atmos.ParseDID(string(val))
	if err != nil {
		return "", false, fmt.Errorf("backfill: lookup handle index %q: invalid DID: %w", handle, err)
	}
	return did, true, nil
}

// LookupDIDByHandle reads the local declared-handle index. The index is
// an operator convenience only; it is not bidirectionally verified.
func LookupDIDByHandle(db *store.Store, handle string) (atmos.DID, bool, error) {
	return lookupDIDByHandle(db, handle)
}

// LoadRepoStatus reads one repo/<did> row.
func LoadRepoStatus(db *store.Store, did atmos.DID) (*RepoStatus, bool, error) {
	val, closer, err := db.Get(repoKey(did))
	if errors.Is(err, store.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load repo status %s: %w", did, err)
	}
	defer func() { _ = closer.Close() }()
	rs, err := decodeRepoStatus(val)
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load repo status %s: %w", did, err)
	}
	return rs, true, nil
}

// LoadHostStatus reads one host/<bucket> row.
func LoadHostStatus(db *store.Store, host string) (*HostStatus, bool, error) {
	return loadHostStatus(db, host)
}

// ListHostStatuses scans the maintained host aggregate keyspace. This is
// bounded by the number of PDS host buckets, not the number of accounts.
func ListHostStatuses(db *store.Store) ([]HostStatus, error) {
	prefix := []byte(hostKeyPrefix)
	it, err := db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: store.PrefixUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("backfill: open host status iter: %w", err)
	}
	defer func() { _ = it.Close() }()

	var out []HostStatus
	for it.First(); it.Valid(); it.Next() {
		val, err := it.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("backfill: read host status: %w", err)
		}
		hs, err := decodeHostStatus(val)
		if err != nil {
			return nil, err
		}
		key := string(it.Key())
		hs.Host = strings.TrimPrefix(key, hostKeyPrefix)
		out = append(out, *hs)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("backfill: iterate host statuses: %w", err)
	}
	return out, nil
}

func normalizeHostBucket(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false
	}

	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", false
	}

	port := u.Port()
	if port == "" || (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
		return host, true
	}
	if strings.Contains(host, ":") {
		return net.JoinHostPort(host, port), true
	}
	return host + ":" + port, true
}

// hostBucketFromAuthority normalizes a bare "host" or "host:port"
// authority — as surfaced by atmos on OnComplete/OnFail from the final
// (post-redirect) request URL — into the same host-bucket key space
// that normalizeHostBucket produces from a full URL. The default https
// port is stripped so "pds.example.com:443" and "pds.example.com" share
// a bucket; non-default ports are preserved. Returns ("", false) for an
// empty authority (e.g. a dial failure that never reached a server).
func hostBucketFromAuthority(authority string) (string, bool) {
	authority = strings.ToLower(strings.TrimSpace(authority))
	if authority == "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		// No port present: use the authority as-is.
		return authority, true
	}
	if host == "" {
		return "", false
	}
	// Strip the default https port; backfill downloads are https.
	if port == "" || port == "443" {
		return host, true
	}
	return net.JoinHostPort(host, port), true
}

func classifyBackfillError(err error) ErrorClass {
	if err == nil {
		return ErrorClassUnknown
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassTimeout
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "identity:") || strings.Contains(msg, "did not found"):
		return ErrorClassDIDResolution
	case strings.Contains(msg, "atprotopersonaldataserver"):
		return ErrorClassInvalidPDS
	case http429ErrorRE.MatchString(msg):
		return ErrorClassHTTP429
	case http5xxErrorRE.MatchString(msg):
		return ErrorClassHTTP5xx
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return ErrorClassTimeout
	case strings.Contains(msg, "car:"):
		return ErrorClassCAR
	case strings.Contains(msg, "verify") || strings.Contains(msg, "signature"):
		return ErrorClassVerification
	case strings.Contains(msg, "flush before complete") || strings.Contains(msg, "write repo/") || strings.Contains(msg, "disk full"):
		return ErrorClassLocalWrite
	default:
		return ErrorClassUnknown
	}
}

// isRepoNotFoundError reports whether err is the terminal "this repo
// does not exist" signal from com.atproto.sync.getRepo. The canonical
// lexicon error name is RepoNotFound, but production bsky.network hosts
// report it as a generic "NotFound" name with a "Repo not found"
// message instead. Both mean the same thing — there is nothing to
// download and never will be — so we must not retry or count either as
// a failed host. The generic "NotFound" name is gated on the message so
// an unrelated NotFound (a missing record or blob) can't be mistaken
// for a missing repo.
func isRepoNotFoundError(err error) bool {
	var xerr *xrpc.Error
	if !errors.As(err, &xerr) {
		return false
	}
	if xerr.Name == "RepoNotFound" {
		return true
	}
	return xerr.Name == "NotFound" && repoNotFoundMessageRE.MatchString(xerr.Message)
}

// isRepoUnavailableError reports whether err is a terminal "the account
// exists but its repo can't be fetched" signal from getRepo. These are
// the non-RepoNotFound members of the com.atproto.sync.getRepo error
// set: the account was deactivated, suspended, or taken down. They are
// not download failures — the upstream deliberately has no repo to
// serve — so they must not be retried or counted as failed hosts.
//
// RepoNotFound is intentionally excluded; it keeps its own dedicated
// StatusComplete handling in OnFail.
func isRepoUnavailableError(err error) bool {
	var xerr *xrpc.Error
	if !errors.As(err, &xerr) {
		return false
	}
	switch xerr.Name {
	case "RepoDeactivated", "RepoSuspended", "RepoTakendown":
		return true
	default:
		return false
	}
}

func shouldLogBackfillError(err error) bool {
	return !isRepoNotFoundError(err) && !isRepoUnavailableError(err)
}

func truncateErrorString(s string) string {
	if len(s) <= maxStoredErrorBytes {
		return s
	}
	return s[:maxStoredErrorBytes]
}

func (s *HostStatus) addErrorSample(sample HostErrorSample) {
	sample.Error = truncateErrorString(sample.Error)
	s.LatestError = sample.Error
	s.LatestErrorClass = sample.Class
	if s.ErrorClassCounts == nil {
		s.ErrorClassCounts = make(map[ErrorClass]uint64)
	}
	s.ErrorClassCounts[sample.Class]++
	s.RecentErrors = append([]HostErrorSample{sample}, s.RecentErrors...)
	if len(s.RecentErrors) > maxHostErrorSamples {
		s.RecentErrors = s.RecentErrors[:maxHostErrorSamples]
	}
}

func decrementStatus(h *HostStatus, st Status) {
	if p := hostStatusBucket(h, st); p != nil && *p > 0 {
		*p--
	}
}

func incrementStatus(h *HostStatus, st Status) {
	if p := hostStatusBucket(h, st); p != nil {
		*p++
	}
}

func hostStatusBucket(h *HostStatus, st Status) *uint64 {
	switch st {
	case StatusNotStarted:
		return &h.NotStarted
	case StatusPending:
		return &h.Pending
	case StatusComplete:
		return &h.Complete
	case StatusFailed:
		return &h.Failed
	case StatusUnavailable:
		return &h.Unavailable
	default:
		return nil
	}
}

func newHostStatus(host string) *HostStatus {
	return &HostStatus{
		Host:             host,
		ErrorClassCounts: make(map[ErrorClass]uint64),
	}
}

func decodeHostStatus(b []byte) (*HostStatus, error) {
	var s HostStatus
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("backfill: decode HostStatus: %w", err)
	}
	if s.ErrorClassCounts == nil {
		s.ErrorClassCounts = make(map[ErrorClass]uint64)
	}
	return &s, nil
}

func encodeHostStatus(s *HostStatus) ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("backfill: encode HostStatus: %w", err)
	}
	return b, nil
}

// EncodeHostStatus marshals a HostStatus. It is a test-only seed helper,
// NOT production or repair tooling: tests use it to encode a HostStatus row
// they write directly into the store.
func EncodeHostStatus(s *HostStatus) ([]byte, error) {
	return encodeHostStatus(s)
}

func loadHostStatus(db *store.Store, host string) (*HostStatus, bool, error) {
	normalized, key, err := normalizeHostStatusKey(host)
	if err != nil {
		return nil, false, err
	}
	val, closer, err := db.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		return newHostStatus(normalized), false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load host status %q: %w", host, err)
	}
	defer func() { _ = closer.Close() }()

	s, err := decodeHostStatus(val)
	if err != nil {
		return nil, false, fmt.Errorf("backfill: load host status %q: %w", host, err)
	}
	s.Host = normalized
	return s, true, nil
}

func stageHostStatus(batch *pebble.Batch, s *HostStatus) error {
	if s == nil {
		return fmt.Errorf("backfill: stage host status: nil status")
	}
	normalized, key, err := normalizeHostStatusKey(s.Host)
	if err != nil {
		return err
	}
	normalizedStatus := *s
	normalizedStatus.Host = normalized
	if normalizedStatus.ErrorClassCounts == nil {
		normalizedStatus.ErrorClassCounts = make(map[ErrorClass]uint64)
	}
	enc, err := encodeHostStatus(&normalizedStatus)
	if err != nil {
		return err
	}
	if err := batch.Set(key, enc, nil); err != nil {
		return fmt.Errorf("backfill: stage host status %q: %w", s.Host, err)
	}
	return nil
}

package web

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/jetstream/internal/format"
	"github.com/bluesky-social/jetstream/internal/repoexport"
	"github.com/bluesky-social/jetstream/internal/status"
	"github.com/jcalabro/atmos"
)

//go:embed templates/status.html
var templateFS embed.FS

// Snapshotter is what the handler needs from a status collector. The
// concrete *status.Collector satisfies it; tests pass a fake.
type Snapshotter interface {
	SnapshotForRequest(ctx context.Context, req status.Request) (*status.Snapshot, error)
}

// RepoActions provides the expensive, operator-triggered repo actions exposed
// from the status UI. Tests pass a fake; production uses repoexport.
type RepoActions interface {
	VerifyRepo(ctx context.Context, did string) (repoexport.VerifyReport, error)
}

// Handler renders the public /status page. Construct via New.
type Handler struct {
	tpl         *template.Template
	src         Snapshotter
	repoActions RepoActions
	limiter     *repoActionLimiter
	now         func() time.Time
	logger      *slog.Logger
}

// Options configures Handler. Now is overridable for tests.
type Options struct {
	Snapshotter Snapshotter
	RepoActions RepoActions
	// RepoActionRateLimit bounds expensive explicit repo actions by source IP.
	// A zero value uses the production default when RepoActions is configured.
	RepoActionRateLimit RateLimit
	// DisableRepoActionRateLimit bypasses RepoActionRateLimit. It is intended
	// for trusted local/operator deployments where repeated verification is
	// more valuable than protecting the endpoint from accidental load.
	DisableRepoActionRateLimit bool
	Now                        func() time.Time
	// Logger is used to surface post-headers template execution
	// failures (the only place we can't return an HTTP error). nil
	// defaults to slog.Default().
	Logger *slog.Logger
}

// New parses templates at construction time so a malformed template
// surfaces at startup, not on first request.
func New(opts Options) (*Handler, error) {
	if opts.Snapshotter == nil {
		return nil, errors.New("web: Options.Snapshotter is required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.RepoActionRateLimit.Window < 0 || opts.RepoActionRateLimit.Limit < 0 {
		return nil, errors.New("web: repo action rate limit must be non-negative")
	}
	if opts.RepoActions != nil && !opts.DisableRepoActionRateLimit && opts.RepoActionRateLimit.Limit == 0 && opts.RepoActionRateLimit.Window == 0 {
		opts.RepoActionRateLimit = defaultRepoActionRateLimit
	}
	if opts.RepoActions != nil && !opts.DisableRepoActionRateLimit && opts.RepoActionRateLimit.Limit == 0 && opts.RepoActionRateLimit.Window > 0 {
		return nil, errors.New("web: repo action rate limit must include a positive limit")
	}
	if opts.RepoActions != nil && !opts.DisableRepoActionRateLimit && opts.RepoActionRateLimit.Limit > 0 && opts.RepoActionRateLimit.Window <= 0 {
		return nil, errors.New("web: repo action rate limit window is required")
	}
	var limiter *repoActionLimiter
	if opts.RepoActions != nil && !opts.DisableRepoActionRateLimit && opts.RepoActionRateLimit.Limit > 0 {
		limiter = newRepoActionLimiter(opts.RepoActionRateLimit, opts.Now)
	}

	funcs := template.FuncMap{
		"humanBytes":    format.Bytes,
		"humanDuration": humanDuration,
		"humanInt":      format.Int,
		"humanInt64":    func(n int64) string { return format.Int(uint64(n)) },
		"humanInt64Cast": func(n any) string {
			switch v := n.(type) {
			case uint32:
				return format.Int(uint64(v))
			case uint64:
				return format.Int(v)
			case int:
				return format.Int(uint64(v))
			default:
				return fmt.Sprint(n)
			}
		},
		"relativeTime": relativeTime,
		"dict":         dictFunc,
	}

	tpl, err := template.New("status.html").Funcs(funcs).ParseFS(templateFS, "templates/status.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse template: %w", err)
	}

	return &Handler{
		tpl:         tpl,
		src:         opts.Snapshotter,
		repoActions: opts.RepoActions,
		limiter:     limiter,
		now:         opts.Now,
		logger:      opts.Logger,
	}, nil
}

// dictFunc exists because Go templates can only pass a single pipeline
// value into a sub-template ({{template "x" .Foo}}). When a sub-template
// needs several named values, the caller builds them into a map inline
// ({{template "x" dict "a" .A "b" .B}}) and the sub-template reads them
// back by key. Keys must be strings and arguments must come in pairs.
func dictFunc(kv ...any) (map[string]any, error) {
	if len(kv)%2 != 0 {
		return nil, errors.New("dict: odd number of args")
	}
	m := make(map[string]any, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			return nil, errors.New("dict: keys must be strings")
		}
		m[k] = kv[i+1]
	}
	return m, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/status", "":
		h.serveStatus(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) serveStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reqStatus := status.Request{
		Tab:      r.URL.Query().Get("tab"),
		Account:  r.URL.Query().Get("account"),
		DID:      r.URL.Query().Get("did"),
		Handle:   r.URL.Query().Get("handle"),
		HostSort: r.URL.Query().Get("sort"),
	}

	snap, err := h.src.SnapshotForRequest(r.Context(), reqStatus)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<!doctype html><meta charset="utf-8"><title>jetstream</title><p>Status temporarily unavailable.</p>`))
		return
	}

	var verification *repoexport.VerifyReport
	var verificationErr string
	verifyDID := h.autoVerifyDID(r, snap)
	if verifyDID != "" {
		if !h.allowRepoAction(r) {
			verificationErr = "repo action rate limit exceeded"
		} else {
			report, err := h.repoActions.VerifyRepo(r.Context(), verifyDID)
			if err != nil {
				verificationErr = err.Error()
			} else {
				verification = &report
			}
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Status-Generated-At", snap.GeneratedAt.UTC().Format(time.RFC3339Nano))

	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	data := struct {
		*status.Snapshot
		Now                   time.Time
		RepoVerification      *repoexport.VerifyReport
		RepoVerificationError string
	}{
		Snapshot:              snap,
		Now:                   h.now(),
		RepoVerification:      verification,
		RepoVerificationError: verificationErr,
	}
	if err := h.tpl.Execute(w, data); err != nil {
		// Headers already written; we can't change the status now.
		// Surface via slog so a malformed template (vs. a connection
		// drop) doesn't go silent.
		h.logger.Warn("status: render failed", "err", err)
	}
}

func (h *Handler) autoVerifyDID(r *http.Request, snap *status.Snapshot) string {
	if r.Method != http.MethodGet || h.repoActions == nil || snap == nil {
		return ""
	}
	if snap.Request.Tab != "accounts" || !snap.Account.Found {
		return ""
	}
	did, err := validateRepoDID(snap.Account.DID)
	if err != nil {
		return ""
	}
	return did
}

func (h *Handler) allowRepoAction(r *http.Request) bool {
	if h.limiter == nil {
		return true
	}
	return h.limiter.allow(sourceIP(r.RemoteAddr))
}

func validateRepoDID(raw string) (string, error) {
	did := strings.TrimSpace(raw)
	if did == "" {
		return "", errors.New("repo action requires did")
	}
	parsed, err := atmos.ParseDID(did)
	if err != nil {
		return "", fmt.Errorf("invalid did: %w", err)
	}
	return string(parsed), nil
}

func sourceIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil && host != "" {
		return host
	}
	return remoteAddr
}

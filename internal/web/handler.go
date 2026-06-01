package web

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/jetstream-v2/internal/status"
)

//go:embed templates/status.html
var templateFS embed.FS

// Snapshotter is what the handler needs from a status collector. The
// concrete *status.Collector satisfies it; tests pass a fake.
type Snapshotter interface {
	Snapshot(ctx context.Context) (*status.Snapshot, error)
}

// Handler renders the public /status page. Construct via New.
type Handler struct {
	tpl    *template.Template
	src    Snapshotter
	now    func() time.Time
	logger *slog.Logger
}

// Options configures Handler. Now is overridable for tests.
type Options struct {
	Snapshotter Snapshotter
	Now         func() time.Time
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

	funcs := template.FuncMap{
		"humanBytes":    humanBytes,
		"humanDuration": humanDuration,
		"humanInt":      humanInt,
		"humanInt64":    func(n int64) string { return humanInt(uint64(n)) },
		"humanInt64Cast": func(n any) string {
			switch v := n.(type) {
			case uint32:
				return humanInt(uint64(v))
			case uint64:
				return humanInt(v)
			case int:
				return humanInt(uint64(v))
			default:
				return fmt.Sprint(n)
			}
		},
		"relativeTime":  relativeTime,
		"percentString": func(p float64) string { return strconv.FormatFloat(p, 'f', 2, 64) + "%" },
		"dict":          dictFunc,
	}

	tpl, err := template.New("status.html").Funcs(funcs).ParseFS(templateFS, "templates/status.html")
	if err != nil {
		return nil, fmt.Errorf("web: parse template: %w", err)
	}

	return &Handler{tpl: tpl, src: opts.Snapshotter, now: opts.Now, logger: opts.Logger}, nil
}

// dictFunc lets the template build map[string]any inline so we can
// pass multiple values to a sub-template.
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
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap, err := h.src.Snapshot(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`<!doctype html><meta charset="utf-8"><title>jetstream</title><p>Status temporarily unavailable.</p>`))
		return
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
		Now time.Time
	}{
		Snapshot: snap,
		Now:      h.now(),
	}
	if err := h.tpl.Execute(w, data); err != nil {
		// Headers already written; we can't change the status now.
		// Surface via slog so a malformed template (vs. a connection
		// drop) doesn't go silent.
		h.logger.Warn("status: render failed", "err", err)
	}
}

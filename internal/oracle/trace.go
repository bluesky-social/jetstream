package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

type Trace struct {
	mu   sync.Mutex
	next uint64
	w    io.Writer
}

type TraceRecord struct {
	Index uint64         `json:"index"`
	Kind  string         `json:"kind"`
	At    string         `json:"at,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
}

func NewTrace(w io.Writer) *Trace {
	return &Trace{w: w}
}

func (t *Trace) Record(kind string, data map[string]any) error {
	if t == nil {
		return nil
	}
	if t.w == nil {
		return errors.New("oracle trace: nil writer")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.next++
	record := TraceRecord{
		Index: t.next,
		Kind:  kind,
		Data:  data,
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	n, err := t.w.Write(line)
	if err != nil {
		return err
	}
	if n != len(line) {
		return io.ErrShortWrite
	}
	return nil
}

func recordTrace(t *Trace, kind string, data map[string]any) error {
	if t == nil {
		return nil
	}
	return t.Record(kind, data)
}

func tracePayload(payload []byte) map[string]any {
	sum := sha256.Sum256(payload)
	return map[string]any{
		"len":       len(payload),
		"sha256_64": hex.EncodeToString(sum[:8]),
	}
}

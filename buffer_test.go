package jetstream

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func frame(seq uint64, body string) LiveFrame {
	return LiveFrame{Seq: seq, Data: []byte(`{"seq":` + itoa(seq) + `,"x":"` + body + `"}`)}
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func drain(t *testing.T, b LiveBuffer, from uint64) []LiveFrame {
	t.Helper()
	var out []LiveFrame
	for fr, err := range b.Replay(context.Background(), from) {
		require.NoError(t, err)
		out = append(out, fr)
	}
	return out
}

// bufferContract exercises the behavior every LiveBuffer must satisfy.
func bufferContract(t *testing.T, mk func(t *testing.T) LiveBuffer) {
	t.Helper()

	t.Run("append and replay in order", func(t *testing.T) {
		b := mk(t)
		require.NoError(t, b.Append([]LiveFrame{frame(1, "a"), frame(2, "b")}))
		require.NoError(t, b.Append([]LiveFrame{frame(3, "c")}))
		got := drain(t, b, 0)
		require.Equal(t, []uint64{1, 2, 3}, frameSeqs(got))
		require.Equal(t, `{"seq":2,"x":"b"}`, string(got[1].Data))
	})

	t.Run("replay from skips at-or-below", func(t *testing.T) {
		b := mk(t)
		require.NoError(t, b.Append([]LiveFrame{frame(1, "a"), frame(2, "b"), frame(3, "c")}))
		got := drain(t, b, 2)
		require.Equal(t, []uint64{3}, frameSeqs(got))
	})

	t.Run("truncate drops at-or-below", func(t *testing.T) {
		b := mk(t)
		require.NoError(t, b.Append([]LiveFrame{frame(1, "a"), frame(2, "b"), frame(3, "c"), frame(4, "d")}))
		require.NoError(t, b.Truncate(2))
		got := drain(t, b, 0)
		require.Equal(t, []uint64{3, 4}, frameSeqs(got))
		// Appending after truncate continues to work.
		require.NoError(t, b.Append([]LiveFrame{frame(5, "e")}))
		require.Equal(t, []uint64{3, 4, 5}, frameSeqs(drain(t, b, 0)))
	})

	t.Run("empty replay", func(t *testing.T) {
		b := mk(t)
		require.Empty(t, drain(t, b, 0))
	})
}

func TestMemLiveBufferContract(t *testing.T) {
	t.Parallel()
	bufferContract(t, func(t *testing.T) LiveBuffer { return NewMemLiveBuffer() })
}

func TestFileLiveBufferContract(t *testing.T) {
	t.Parallel()
	bufferContract(t, func(t *testing.T) LiveBuffer {
		b, err := NewFileLiveBuffer(filepath.Join(t.TempDir(), "live.jsonl"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		return b
	})
}

func TestFileLiveBufferPersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "live.jsonl")

	b, err := NewFileLiveBuffer(path)
	require.NoError(t, err)
	require.NoError(t, b.Append([]LiveFrame{frame(1, "a"), frame(2, "b")}))
	require.NoError(t, b.Close()) // flushes + fsyncs

	reopened, err := NewFileLiveBuffer(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, []uint64{1, 2}, frameSeqs(drain(t, reopened, 0)))
}

func TestFileLiveBufferRecoversFromPartialTrailingLine(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "live.jsonl")
	b, err := NewFileLiveBuffer(path)
	require.NoError(t, err)
	require.NoError(t, b.Append([]LiveFrame{frame(1, "a"), frame(2, "b")}))
	require.NoError(t, b.Close())

	// Simulate a crash mid-append: a partial line with no trailing newline.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString("3 {\"seq\":3,\"x\":\"par") // truncated, no newline
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reopened, err := NewFileLiveBuffer(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	// The complete frames survive; the partial trailing line is discarded.
	require.Equal(t, []uint64{1, 2}, frameSeqs(drain(t, reopened, 0)))
}

func TestFileLiveBufferRejectsNewlineInFrame(t *testing.T) {
	t.Parallel()
	b, err := NewFileLiveBuffer(filepath.Join(t.TempDir(), "live.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	err = b.Append([]LiveFrame{{Seq: 1, Data: []byte("has\nnewline")}})
	require.ErrorContains(t, err, "newline")
}

func TestFileLiveBufferFlushCadence(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "live.jsonl")
	raw, err := NewFileLiveBuffer(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	b, ok := raw.(*fileLiveBuffer)
	require.True(t, ok)

	// Freeze logical time so only the frame-count threshold can trigger flush.
	now := time.Unix(0, 0)
	b.now = func() time.Time { return now }
	b.lastFlush = now

	// Append fewer than the frame threshold: nothing fsynced yet (still buffered).
	for i := uint64(1); i <= 10; i++ {
		require.NoError(t, b.Append([]LiveFrame{frame(i, "x")}))
	}
	require.Equal(t, 10, b.unflushed, "below threshold and within interval: no flush")

	// Advancing past the interval triggers a flush on the next append.
	now = now.Add(fileBufferFlushInterval + time.Second)
	require.NoError(t, b.Append([]LiveFrame{frame(11, "x")}))
	require.Zero(t, b.unflushed, "interval elapsed: flushed")

	// The frame-count threshold also triggers a flush.
	now = now.Add(time.Millisecond) // still within interval
	for i := uint64(12); i < 12+fileBufferFlushFrames; i++ {
		require.NoError(t, b.Append([]LiveFrame{frame(i, "x")}))
	}
	require.Zero(t, b.unflushed, "frame-count threshold: flushed")
}

func frameSeqs(frames []LiveFrame) []uint64 {
	out := make([]uint64, len(frames))
	for i := range frames {
		out[i] = frames[i].Seq
	}
	return out
}

package main

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

var errTestWrite = errors.New("write failed")

func TestDecodeEventKeyDistinguishesOpsInSameCommit(t *testing.T) {
	t.Parallel()

	first := mustDecodeObservation(t, sourceV1, []byte(`{"did":"did:plc:abc","kind":"commit","commit":{"rev":"3abc","operation":"create","collection":"app.bsky.feed.post","rkey":"one"}}`))
	second := mustDecodeObservation(t, sourceV1, []byte(`{"did":"did:plc:abc","kind":"commit","commit":{"rev":"3abc","operation":"create","collection":"app.bsky.feed.post","rkey":"two"}}`))

	if !first.keyed || !second.keyed {
		t.Fatal("expected keyed commit observations")
	}
	if first.key == second.key {
		t.Fatalf("same DID/rev with different rkey collapsed to one key: %+v", first.key)
	}
}

func TestNormalizePayloadIgnoresTopLevelTimeUSAndCursor(t *testing.T) {
	t.Parallel()

	v1, err := normalizePayload([]byte(`{"did":"did:plc:abc","time_us":1781684500192009,"kind":"commit","commit":{"rev":"3abc","operation":"delete","collection":"app.bsky.feed.post","rkey":"x"}}`))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := normalizePayload([]byte(`{"did":"did:plc:abc","time_us":1781684495799548,"cursor":469083,"kind":"commit","commit":{"rev":"3abc","operation":"delete","collection":"app.bsky.feed.post","rkey":"x"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v1, v2) {
		t.Fatalf("normalized payloads differ\nv1=%s\nv2=%s", v1, v2)
	}
}

func TestWarmupEventsCanSatisfySampleCounterparts(t *testing.T) {
	t.Parallel()

	c := newComparator(10)
	key := eventKey{DID: "did:plc:abc", Rev: "3abc", Operation: "create", Collection: "app.bsky.feed.post", RKey: "one"}

	c.observe(phaseWarmup, keyedObservation(sourceV2, key, `{"side":"v2"}`, time.Unix(0, 1)))
	c.observe(phaseSample, keyedObservation(sourceV1, key, `{"side":"v2"}`, time.Unix(0, 2)))

	result := c.compare(10, 1)
	if result.common != 1 {
		t.Fatalf("common=%d, want 1", result.common)
	}
	if result.onlyV1Count != 0 || result.onlyV2Count != 0 || result.mismatchCount != 0 {
		t.Fatalf("unexpected failure result: %+v", result)
	}
}

func TestGraceEventsCanSatisfySampleCounterparts(t *testing.T) {
	t.Parallel()

	c := newComparator(10)
	key := eventKey{DID: "did:plc:abc", Rev: "3abc", Operation: "create", Collection: "app.bsky.feed.post", RKey: "one"}

	c.observe(phaseSample, keyedObservation(sourceV1, key, `{"same":true}`, time.Unix(0, 1)))
	c.observe(phaseDrain, keyedObservation(sourceV2, key, `{"same":true}`, time.Unix(0, 2)))

	result := c.compare(10, 1)
	if result.common != 1 {
		t.Fatalf("common=%d, want 1", result.common)
	}
	if result.failed() {
		t.Fatalf("grace counterpart should make sample pass: %+v", result)
	}
}

func TestEventsFirstSeenDuringGraceDoNotEnterSample(t *testing.T) {
	t.Parallel()

	c := newComparator(10)
	key := eventKey{DID: "did:plc:abc", Rev: "3abc", Operation: "create", Collection: "app.bsky.feed.post", RKey: "one"}

	c.observe(phaseDrain, keyedObservation(sourceV1, key, `{"same":true}`, time.Unix(0, 1)))
	c.observe(phaseDrain, keyedObservation(sourceV2, key, `{"same":true}`, time.Unix(0, 2)))

	result := c.compare(10, 0)
	if result.sampleKeys != 0 || result.common != 0 {
		t.Fatalf("drain-only event entered sample: %+v", result)
	}
}

func TestConfirmedMissingFiresOnlyAfterGrace(t *testing.T) {
	t.Parallel()

	c := newComparator(10)
	key := eventKey{DID: "did:plc:abc", Rev: "3abc", Operation: "create", Collection: "app.bsky.feed.post", RKey: "one"}

	c.observe(phaseSample, keyedObservation(sourceV1, key, `{"side":"v1"}`, time.Unix(0, 1)))

	result := c.compare(10, 1)
	if result.onlyV1Count != 1 || result.onlyV2Count != 0 {
		t.Fatalf("missing counts = v1:%d v2:%d, want v1:1 v2:0", result.onlyV1Count, result.onlyV2Count)
	}
	if !result.failed() {
		t.Fatal("confirmed missing sample event should fail")
	}
}

func TestInsufficientSampleFails(t *testing.T) {
	t.Parallel()

	c := newComparator(10)
	result := c.compare(10, 1)
	if !result.insufficientSample {
		t.Fatalf("empty sample should be insufficient: %+v", result)
	}
	if !result.failed() {
		t.Fatal("insufficient sample should fail")
	}
}

func TestPrintReportReturnsWriteError(t *testing.T) {
	t.Parallel()

	err := printReport(failingWriter{}, newComparator(1), comparisonResult{}, compareConfig{})
	if !errors.Is(err, errTestWrite) {
		t.Fatalf("printReport error = %v, want %v", err, errTestWrite)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

func mustDecodeObservation(t *testing.T, src source, raw []byte) observation {
	t.Helper()
	obs, err := decodeObservation(src, raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return obs
}

func keyedObservation(src source, key eventKey, payload string, seenAt time.Time) observation {
	return observation{
		source:     src,
		key:        key,
		normalized: []byte(payload),
		seenAt:     seenAt,
		keyed:      true,
	}
}

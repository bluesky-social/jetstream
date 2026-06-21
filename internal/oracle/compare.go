package oracle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// Compare reports the first final-state mismatch between ground truth and
// reconstructed oracle models.
func Compare(want, got *Model) error {
	wantAccounts := accountsOf(want)
	gotAccounts := accountsOf(got)
	if err := validateModel("want", wantAccounts); err != nil {
		return err
	}
	if err := validateModel("got", gotAccounts); err != nil {
		return err
	}

	for _, did := range sortedDIDs(wantAccounts) {
		wantRepo := wantAccounts[did]
		gotRepo := gotAccounts[did]
		wantRecords := recordsOf(wantRepo)
		gotRecords := recordsOf(gotRepo)

		for _, key := range sortedKeys(wantRecords) {
			wantVal := wantRecords[key]
			gotVal, ok := gotRecords[key]
			if !ok {
				return fmt.Errorf("oracle: missing %s %s/%s rev=%s", key.DID, key.Collection, key.Rkey, wantVal.Rev)
			}
			// Rev is deliberately NOT compared here. Ground truth derived from
			// world repo state cannot populate a correct per-record rev: the
			// MST exposes record bytes keyed by path, not the commit rev that
			// last wrote each record, and the world tracks only the repo head
			// rev (see GroundTruthFromWorld / model.go RecordValue.Rev). Worse,
			// a per-record rev would FALSE-POSITIVE on every #sync-resynced DID:
			// reconstruct collapses all of a DID's records to the single sync
			// rev (KindCreateResync), while the world retains each record's
			// original creation rev — reconciling the two would require
			// replaying the firehose into ground truth, which breaks oracle
			// independence (ground truth must come from world state, never the
			// event stream). Per-event rev correctness is therefore owned by
			// the EVENT-LOG tier (NormalizeEventLog/CompareEventLog* compare the
			// rev field), which now covers the restart phase too via the #113
			// chain intermediates. RecordValue.Rev is retained only for
			// diagnostics (payloadMismatchDetail) and the reconstruct fold.
			if !bytes.Equal(wantVal.Payload, gotVal.Payload) {
				return fmt.Errorf("oracle: payload mismatch %s %s/%s: %s",
					key.DID, key.Collection, key.Rkey, payloadMismatchDetail(wantVal, gotVal))
			}
		}
		for _, key := range sortedKeys(gotRecords) {
			if _, ok := wantRecords[key]; !ok {
				return fmt.Errorf("oracle: extra %s %s/%s", key.DID, key.Collection, key.Rkey)
			}
		}
	}

	for _, did := range sortedDIDs(gotAccounts) {
		gotRecords := recordsOf(gotAccounts[did])
		if _, ok := wantAccounts[did]; !ok && len(gotRecords) > 0 {
			return fmt.Errorf("oracle: extra account %s with %d records", did, len(gotRecords))
		}
	}
	return nil
}

func validateModel(label string, accounts map[string]RepoSnapshot) error {
	for _, did := range sortedDIDs(accounts) {
		for _, key := range sortedKeys(recordsOf(accounts[did])) {
			if key.DID != did {
				return fmt.Errorf("oracle: %s account %s contains record key for %s %s/%s",
					label, did, key.DID, key.Collection, key.Rkey)
			}
		}
	}
	return nil
}

func accountsOf(model *Model) map[string]RepoSnapshot {
	if model == nil || model.Accounts == nil {
		return nil
	}
	return model.Accounts
}

func recordsOf(repo RepoSnapshot) map[RecordKey]RecordValue {
	return repo.Records
}

func sortedDIDs(accounts map[string]RepoSnapshot) []string {
	dids := make([]string, 0, len(accounts))
	for did := range accounts {
		dids = append(dids, did)
	}
	sort.Strings(dids)
	return dids
}

func sortedKeys(records map[RecordKey]RecordValue) []RecordKey {
	keys := make([]RecordKey, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].DID != keys[j].DID {
			return keys[i].DID < keys[j].DID
		}
		if keys[i].Collection != keys[j].Collection {
			return keys[i].Collection < keys[j].Collection
		}
		return keys[i].Rkey < keys[j].Rkey
	})
	return keys
}

func payloadMismatchDetail(want, got RecordValue) string {
	wantHash := sha256.Sum256(want.Payload)
	gotHash := sha256.Sum256(got.Payload)
	return fmt.Sprintf("want len=%d rev=%q sha256=%s sample=%s got len=%d rev=%q sha256=%s sample=%s first_diff=%d",
		len(want.Payload), want.Rev, hex.EncodeToString(wantHash[:8]), payloadSample(want.Payload),
		len(got.Payload), got.Rev, hex.EncodeToString(gotHash[:8]), payloadSample(got.Payload),
		firstDiffOffset(want.Payload, got.Payload))
}

func firstDiffOffset(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return n
	}
	return -1
}

func payloadSample(payload []byte) string {
	const maxSampleBytes = 16
	if len(payload) <= maxSampleBytes {
		return hex.EncodeToString(payload)
	}
	return hex.EncodeToString(payload[:maxSampleBytes]) + "..."
}

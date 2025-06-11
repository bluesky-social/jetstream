package server

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseMaxMessageSizeBytes(t *testing.T) {
	stringTests := []struct {
		name                string
		maxMessageSizeBytes string
		expected            uint32
	}{
		{
			name:                "zero",
			maxMessageSizeBytes: "0",
			expected:            0,
		},
		{
			name:                "empty",
			maxMessageSizeBytes: "",
			expected:            0,
		},
		{
			name:                "invalid",
			maxMessageSizeBytes: "nope",
			expected:            0,
		},
		{
			name:                "valid",
			maxMessageSizeBytes: "1000000",
			expected:            1000000,
		},
		{
			name:                "uint32max",
			maxMessageSizeBytes: "4294967295",
			expected:            4294967295,
		},
		{
			name:                "uint32max_plus1",
			maxMessageSizeBytes: "4294967296",
			expected:            0,
		},
	}

	for _, tt := range stringTests {
		t.Run(fmt.Sprintf("string %s", tt.name), func(t *testing.T) {
			got := ParseMaxMessageSizeBytes(tt.maxMessageSizeBytes)
			if got != tt.expected {
				t.Errorf("expected max size to be %d, got %d", tt.expected, got)
			}
		})
	}

	intTests := []struct {
		name                string
		maxMessageSizeBytes int
		expected            uint32
	}{
		{
			name:                "zero",
			maxMessageSizeBytes: 0,
			expected:            0,
		},
		{
			name:                "uint32max",
			maxMessageSizeBytes: 4294967295,
			expected:            4294967295,
		},
		{
			name:                "uint32max_plus1",
			maxMessageSizeBytes: 4294967296,
			expected:            0,
		},
	}

	for _, tt := range intTests {
		t.Run(fmt.Sprintf("int %s", tt.name), func(t *testing.T) {
			got := ParseMaxMessageSizeBytes(tt.maxMessageSizeBytes)
			if got != tt.expected {
				t.Errorf("expected max size to be %d, got %d", tt.expected, got)
			}
		})
	}
}

func TestParseSubscriberOptions(t *testing.T) {
	testCases := []struct {
		name     string
		data     []byte
		expected SubscriberOptionsUpdatePayload
	}{
		{
			name: "empty",
			data: []byte(`{}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections:   nil,
				WantedDIDs:          nil,
				MaxMessageSizeBytes: 0,
			},
		},
		{
			name: "collection",
			data: []byte(`{"wantedCollections":["foo"]}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections:   []string{"foo"},
				WantedDIDs:          nil,
				MaxMessageSizeBytes: 0,
			},
		},
		{
			name: "small",
			data: []byte(`{"maxMessageSizeBytes":1000}`),
			expected: SubscriberOptionsUpdatePayload{
				WantedCollections:   nil,
				WantedDIDs:          nil,
				MaxMessageSizeBytes: 1000,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var subOptsUpdate SubscriberOptionsUpdatePayload
			if err := json.Unmarshal(testCase.data, &subOptsUpdate); err != nil {
				t.Errorf("failed to unmarshal subscriber options update: %v", err)
			}
			assert.Equal(t, subOptsUpdate, testCase.expected)
		})
	}

}

func TestSharder(t *testing.T) {
	type testCase struct {
		name        string
		input       url.Values
		mask, index uint64
		error       string

		valid   []string
		invalid []string
	}

	testCases := []testCase{
		{
			input: url.Values{},
			valid: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"},
		},
		{
			input: url.Values{
				"shardingCount": []string{"foo"},
			},
			error: `shardingCount: strconv.Atoi: parsing "foo": invalid syntax`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"4"},
			},
			error: `shardingIndex: must be specified when shardingCount is specified`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"4"},
				"shardingIndex": []string{"foo"},
			},
			error: `shardingIndex: strconv.Atoi: parsing "foo": invalid syntax`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"5"},
				"shardingIndex": []string{"3"},
			},
			error: `error building sharder: count needs to be a power of two`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"-4"},
				"shardingIndex": []string{"3"},
			},
			error: `error building sharder: count needs to be positive`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"4"},
				"shardingIndex": []string{"4"},
			},
			error: `error building sharder: index needs to be in [0, count)`,
		},
		{
			input: url.Values{
				"shardingCount": []string{"4"},
				"shardingIndex": []string{"3"},
			},
			mask:    0b11,
			index:   3,
			valid:   []string{"2", "6", "7", "a", "b"}, // hashing returns 5 valid out of 16
			invalid: []string{"0", "1", "3", "4", "5", "8", "9", "c", "d", "e", "f"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			s, err := NewSharderFromURL(tc.input)
			if tc.error != "" {
				assert.NotNil(err)
				assert.Equal(tc.error, err.Error())
			} else {
				assert.NoError(err)
			}

			if tc.mask != 0 {
				assert.Equal(tc.index, s.Index)
				assert.Equal(tc.mask, s.Mask)
			} else {
				assert.Nil(s)
			}

			for _, valid := range tc.valid {
				assert.True(s.matches(valid), "valid: %s", valid)
			}

			for _, invalid := range tc.invalid {
				assert.False(s.matches(invalid), "invalid: %s", invalid)
			}
		})
	}
}

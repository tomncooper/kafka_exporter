package main

import (
	"regexp"
	"testing"
	"time"
)

func newTestExporter(global time.Duration, overrides []topicTimeWindow) *Exporter {
	return &Exporter{
		estimatedTimeLagWindow: global,
		topicTimeWindows:       overrides,
	}
}

func TestWindowForTopic_NoOverrides(t *testing.T) {
	e := newTestExporter(10*time.Minute, nil)
	if got := e.windowForTopic("any-topic"); got != 10*time.Minute {
		t.Errorf("expected 10m, got %v", got)
	}
}

func TestWindowForTopic_MatchingOverride(t *testing.T) {
	e := newTestExporter(5*time.Minute, []topicTimeWindow{
		{pattern: regexp.MustCompile(`high-throughput-.*`), window: 30 * time.Second},
	})
	if got := e.windowForTopic("high-throughput-orders"); got != 30*time.Second {
		t.Errorf("expected 30s, got %v", got)
	}
}

func TestWindowForTopic_NonMatching(t *testing.T) {
	e := newTestExporter(5*time.Minute, []topicTimeWindow{
		{pattern: regexp.MustCompile(`high-throughput-.*`), window: 30 * time.Second},
	})
	if got := e.windowForTopic("low-volume-events"); got != 5*time.Minute {
		t.Errorf("expected 5m, got %v", got)
	}
}

func TestWindowForTopic_FirstMatchWins(t *testing.T) {
	e := newTestExporter(5*time.Minute, []topicTimeWindow{
		{pattern: regexp.MustCompile(`orders-.*`), window: 30 * time.Second},
		{pattern: regexp.MustCompile(`orders-priority`), window: 10 * time.Second},
	})
	if got := e.windowForTopic("orders-priority"); got != 30*time.Second {
		t.Errorf("expected 30s (first match), got %v", got)
	}
}

func TestWindowForTopic_CachesResult(t *testing.T) {
	e := newTestExporter(5*time.Minute, []topicTimeWindow{
		{pattern: regexp.MustCompile(`cached-.*`), window: 1 * time.Minute},
	})
	first := e.windowForTopic("cached-topic")
	second := e.windowForTopic("cached-topic")
	if first != second {
		t.Errorf("cache inconsistency: %v != %v", first, second)
	}
	if _, ok := e.topicWindowCache.Load("cached-topic"); !ok {
		t.Error("expected topic to be cached")
	}
}

func TestWindowForTopic_ExactMatchRegex(t *testing.T) {
	e := newTestExporter(5*time.Minute, []topicTimeWindow{
		{pattern: regexp.MustCompile(`^exact-topic$`), window: 2 * time.Minute},
	})
	if got := e.windowForTopic("exact-topic"); got != 2*time.Minute {
		t.Errorf("expected 2m for exact match, got %v", got)
	}
	if got := e.windowForTopic("exact-topic-extended"); got != 5*time.Minute {
		t.Errorf("expected 5m fallback for non-match, got %v", got)
	}
}

func TestParseTopicTimeWindows(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		wantErr string
	}{
		{
			name:  "valid entry",
			input: []string{"high-throughput-.*:30s"},
		},
		{
			name:  "regex with colons",
			input: []string{"topic:foo:bar:1m"},
		},
		{
			name:    "missing colon",
			input:   []string{"nocolon"},
			wantErr: "expected regex:duration",
		},
		{
			name:    "colon at start",
			input:   []string{":5m"},
			wantErr: "expected regex:duration",
		},
		{
			name:    "invalid regex",
			input:   []string{"[invalid:5m"},
			wantErr: "invalid regex",
		},
		{
			name:    "invalid duration",
			input:   []string{"valid-regex:notaduration"},
			wantErr: "invalid duration",
		},
		{
			name:    "zero duration",
			input:   []string{"topic:0s"},
			wantErr: "duration must be positive",
		},
		{
			name:    "negative duration",
			input:   []string{"topic:-5m"},
			wantErr: "duration must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := kafkaOpts{
				uri:                     []string{"localhost:9092"},
				kafkaVersion:            "2.0.0",
				metadataRefreshInterval: "30s",
				groupMetricsTimeout:     "5m",
				estimatedTimeLagWindow:  "5m",
				emitEstimatedTimeLag:    true,
				topicTimeWindows:        tt.input,
			}
			_, err := NewExporter(opts, ".*", "^$", ".*", "^$")
			if tt.wantErr == "" {
				if err != nil && !isConnectionError(err) {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.wantErr)
				} else if !containsString(err.Error(), tt.wantErr) {
					t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
				}
			}
		})
	}
}

func isConnectionError(err error) bool {
	return containsString(err.Error(), "Error Init Kafka Client") ||
		containsString(err.Error(), "kafka: client has run out of available brokers")
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

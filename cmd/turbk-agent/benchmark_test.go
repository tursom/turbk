package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRunAgentThroughputBenchmarkOutputsComparableMetrics(t *testing.T) {
	var output bytes.Buffer
	opts := agentThroughputBenchmarkOptions{
		Files:              2,
		FileSize:           1024,
		Modes:              []string{"serial", "parallel"},
		MaxChunkCheckBatch: 2,
	}
	if err := runAgentThroughputBenchmark(opts, &output); err != nil {
		t.Fatalf("runAgentThroughputBenchmark() error = %v", err)
	}
	var results []agentThroughputBenchmarkResult
	if err := json.Unmarshal(output.Bytes(), &results); err != nil {
		t.Fatalf("decode benchmark output: %v\n%s", err, output.String())
	}
	if len(results) != 2 {
		t.Fatalf("benchmark results = %d, want 2: %s", len(results), output.String())
	}
	for _, result := range results {
		if result.Files != opts.Files || result.FileSize != opts.FileSize || result.Bytes != int64(opts.Files)*opts.FileSize {
			t.Fatalf("benchmark size fields = %+v", result)
		}
		if result.CheckRequests == 0 || result.UploadRequests == 0 {
			t.Fatalf("benchmark request counters not populated: %+v", result)
		}
		if !result.ManifestEquivalent {
			t.Fatalf("benchmark manifest equivalence is false for mode %s", result.Mode)
		}
		if result.AllocBytes == 0 || result.PeakHeapAllocBytes == 0 {
			t.Fatalf("benchmark memory metrics not populated: %+v", result)
		}
	}
}

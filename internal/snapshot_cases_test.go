// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package internal

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const snapshotCasesPath = "../datatest/snapshot_json.cases"

// TestSnapshotJSONCases replays the hand-authored accept/reject cases that
// other implementations use to match Go's snapshot decoding.
func TestSnapshotJSONCases(t *testing.T) {
	raw, err := os.ReadFile(snapshotCasesPath)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		verdict, payload, ok := strings.Cut(line, " ")
		if !ok {
			t.Fatalf("malformed case %q", line)
		}
		n++
		var decoded Snapshot
		err := json.Unmarshal([]byte(payload), &decoded)
		switch verdict {
		case "accept":
			if err != nil {
				t.Errorf("accept %s: %v", payload, err)
			} else if err := decoded.Validate(); err != nil {
				t.Errorf("accept %s: decoded snapshot invalid: %v", payload, err)
			}
		case "reject":
			if err == nil {
				t.Errorf("reject %s: decoded without error", payload)
			}
		default:
			t.Fatalf("unknown verdict %q", verdict)
		}
	}
	if n == 0 {
		t.Fatal("no cases found")
	}
}

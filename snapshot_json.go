// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"bytes"
	"encoding/json"
	"errors"
)

// UnmarshalJSON decodes and validates a Snapshot. JSON null leaves s unchanged.
// Unknown fields are rejected, so payloads in another format (including the
// pre-snake_case field names) fail instead of being partially decoded.
func (s *Snapshot) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	type snapshot Snapshot
	var decoded snapshot
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&decoded); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("goodhistogram: trailing data after snapshot JSON")
	}
	if err := (*Snapshot)(&decoded).Validate(); err != nil {
		return err
	}
	*s = Snapshot(decoded)
	return nil
}

// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import "encoding/json"

// UnmarshalJSON decodes and validates a Snapshot.
func (s *Snapshot) UnmarshalJSON(data []byte) error {
	type snapshot Snapshot
	var decoded snapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := (*Snapshot)(&decoded).Validate(); err != nil {
		return err
	}
	*s = Snapshot(decoded)
	return nil
}

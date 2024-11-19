// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package position

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
)

const (
	TypeSnapshot IteratorType = iota
	TypeCDC
)

type IteratorType int

type Position struct {
	IteratorType IteratorType `json:"iterator_type"`

	// the record's keys, for the snapshot iterator
	PartitionKey string `json:"partition_key"`
	SortKey      string `json:"sort_key"`

	// the record's sequence number in the stream, for the CDC iterator.
	SequenceNumberMap map[string]string `json:"sequence_number"`

	// Flag to tell the CDC iterator whether to check the record time or not.
	AfterSnapshot bool `json:"after_snapshot"`

	Time time.Time `json:"time"`
}

// ParseRecordPosition parses SDK position and returns Position.
func ParseRecordPosition(p opencdc.Position) (Position, error) {
	var pos Position

	if p == nil {
		return Position{}, nil
	}

	err := json.Unmarshal(p, &pos)
	if err != nil {
		return Position{}, fmt.Errorf("error unmarshalling position: %w", err)
	}

	switch pos.IteratorType {
	case TypeSnapshot, TypeCDC:
		return pos, nil
	default:
		return pos, fmt.Errorf("unknown iterator type")
	}
}

// ToRecordPosition formats and returns opencdc.Position.
func (p Position) ToRecordPosition() (opencdc.Position, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return opencdc.Position{}, fmt.Errorf("could not marshal position: %w", err)
	}

	return b, nil
}

func ConvertToCDCPosition(p opencdc.Position) (opencdc.Position, error) {
	cdcPos, err := ParseRecordPosition(p)
	if err != nil {
		return opencdc.Position{}, fmt.Errorf("could not convert position: %w", err)
	}
	cdcPos.IteratorType = TypeCDC
	return cdcPos.ToRecordPosition()
}

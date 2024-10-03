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
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestParseSDKPosition(t *testing.T) {
	validPosition := Position{
		IteratorType:   TypeCDC,
		PartitionKey:   "key",
		SequenceNumber: "my-sequence-number",
		Time:           time.Time{},
	}

	wrongPosType := Position{
		IteratorType:   3, // non-existent type
		PartitionKey:   "key",
		SequenceNumber: "my-sequence-number",
	}
	is := is.New(t)
	posBytes, err := json.Marshal(validPosition)
	is.NoErr(err)

	wrongPosBytes, err := json.Marshal(wrongPosType)
	is.NoErr(err)

	tests := []struct {
		name        string
		in          opencdc.Position
		want        Position
		expectedErr string
	}{
		{
			name: "valid position",
			in:   opencdc.Position(posBytes),
			want: validPosition,
		},
		{
			name:        "unknown iterator type",
			in:          opencdc.Position(wrongPosBytes),
			expectedErr: "unknown iterator type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			got, err := ParseRecordPosition(tt.in)
			if err != nil {
				is.Equal(err.Error(), tt.expectedErr)
				return
			}
			got.Time = time.Time{}
			is.Equal(got, tt.want)
		})
	}
}

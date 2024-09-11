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
	"bytes"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

func Test_ParseRecordPosition(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      opencdc.Position
		out     Position
	}{
		{
			name:    "snapshot position",
			wantErr: false,
			in:      []byte("test_s"),
			out: Position{
				Key:  "test",
				Type: TypeSnapshot,
			},
		},
		{
			name:    "nil position returns empty Position with default values",
			wantErr: false,
			in:      nil,
			out:     Position{},
		},
		{
			name:    "wrong position format returns error",
			wantErr: true,
			in:      []byte("test"),
			out:     Position{},
		},
		{
			name:    "empty position returns error",
			wantErr: true,
			in:      []byte(""),
			out:     Position{},
		},
		{
			name:    "cdc type position",
			wantErr: false,
			in:      []byte("test_c"),
			out: Position{
				Key:  "test",
				Type: TypeCDC,
			},
		},
		{
			name:    "invalid timestamp returns error",
			wantErr: true,
			in:      []byte("test_invalid"),
			out:     Position{},
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ParseRecordPosition(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRecordPosition() error = %v, wantErr %v", err, tt.wantErr)
			} else if p != tt.out {
				t.Errorf("ParseRecordPosition(): Got : %v,Expected : %v", p, tt.out)
			}
		})
	}
}

func Test_ToRecordPosition(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      Position
		out     opencdc.Position
	}{
		{
			name:    "zero position",
			wantErr: false,
			in: Position{
				Key:  "test",
				Type: TypeSnapshot,
			},
			out: []byte("test_s"),
		},
		{
			name:    "empty position returns the zero value for time.Time",
			wantErr: false,
			in:      Position{},
			out:     []byte("_s"),
		},
		{
			name:    "cdc type position",
			wantErr: false,
			in: Position{
				Key:  "test",
				Type: TypeCDC,
			},
			out: []byte("test_c"),
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p := (tt.in).ToRecordPosition()
			if !bytes.Equal(p, tt.out) {
				t.Errorf("ToRecordPosition(): Got : %v,Expected : %v", p, tt.out)
				return
			}
		})
	}
}

func Test_ConvertSnapshotPositionToCDC(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      opencdc.Position
		out     opencdc.Position
	}{
		{
			name:    "convert snapshot position to cdc",
			wantErr: false,
			in:      []byte("test_s"),
			out:     []byte("test_c"),
		},
		{
			name:    "convert invalid snapshot should produce error",
			wantErr: true,
			in:      []byte("s"),
			out:     []byte(""),
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := ConvertToCDCPosition(tt.in)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertToCDCPosition() error = %v, wantErr %v", err, tt.wantErr)
			} else if !bytes.Equal(p, tt.out) {
				t.Errorf("ConvertToCDCPosition(): Got : %v,Expected : %v", p, tt.out)
			}
		})
	}
}

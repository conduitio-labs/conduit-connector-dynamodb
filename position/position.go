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
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
)

const (
	TypeSnapshot Type = iota
	TypeCDC
)

const (
	snapshotPrefixChar = 's'
	cdcPrefixChar      = 'c'
)

type Type int

type Position struct {
	Type Type
	Key  string
}

func ParseRecordPosition(p opencdc.Position) (Position, error) {
	if p == nil {
		// empty Position would have the fields with their default values
		return Position{}, nil
	}
	s := string(p)
	index := strings.LastIndex(s, "_")
	if index == -1 {
		return Position{}, errors.New("invalid position format, no '_' found")
	}

	if s[index+1] != cdcPrefixChar && s[index+1] != snapshotPrefixChar {
		return Position{}, fmt.Errorf("invalid position format, no '%c' or '%c' after '_'", snapshotPrefixChar, cdcPrefixChar)
	}
	pType := TypeSnapshot
	if s[index+1] == cdcPrefixChar {
		pType = TypeCDC
	}

	return Position{
		Key:  s[:index],
		Type: pType,
	}, nil
}

func (p Position) ToRecordPosition() opencdc.Position {
	char := snapshotPrefixChar
	if p.Type == TypeCDC {
		char = cdcPrefixChar
	}
	return []byte(fmt.Sprintf("%s_%c", p.Key, char))
}

func ConvertToCDCPosition(p opencdc.Position) (opencdc.Position, error) {
	cdcPos, err := ParseRecordPosition(p)
	if err != nil {
		return opencdc.Position{}, err
	}
	cdcPos.Type = TypeCDC
	return cdcPos.ToRecordPosition(), nil
}
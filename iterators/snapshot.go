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

package iterator

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// SnapshotIterator to iterate through DynamoDB items in a specific table.
type SnapshotIterator struct {
	tableName        string
	key              string
	client           *dynamodb.Client
	lastEvaluatedKey map[string]types.AttributeValue
	items            []map[string]types.AttributeValue
	firstIt          bool
	index            int
}

// NewSnapshotIterator initializes a SnapshotIterator starting from the provided position.
func NewSnapshotIterator(tableName string, key string, client *dynamodb.Client, p opencdc.Position) (*SnapshotIterator, error) {
	return &SnapshotIterator{
		tableName:        tableName,
		key:              key,
		client:           client,
		lastEvaluatedKey: nil,
		firstIt:          true,
	}, nil
}

// refreshPage fetches the next page of items from DynamoDB.
func (s *SnapshotIterator) refreshPage(ctx context.Context) error {
	fmt.Println("refreshing page")
	s.items = nil
	s.index = 0

	scanInput := &dynamodb.ScanInput{
		TableName:         aws.String(s.tableName),
		ExclusiveStartKey: s.lastEvaluatedKey,
	}

	result, err := s.client.Scan(ctx, scanInput)
	if err != nil {
		return fmt.Errorf("could not fetch next page: %w", err)
	}

	s.items = result.Items
	s.lastEvaluatedKey = result.LastEvaluatedKey
	fmt.Println("GOT the items: #", len(s.items))

	if len(s.items) == 0 {
		return sdk.ErrBackoffRetry
	}

	return nil
}

// HasNext returns true if there are more items to iterate over.
func (s *SnapshotIterator) HasNext(ctx context.Context) bool {
	if s.index < len(s.items) {
		return true
	}
	if s.lastEvaluatedKey != nil || s.firstIt {
		s.firstIt = false
		err := s.refreshPage(ctx)
		return err == nil
	}
	return false
}

// Next returns the next record in the iterator.
func (s *SnapshotIterator) Next(_ context.Context) (opencdc.Record, error) {
	item := s.items[s.index]
	s.index++

	mp := s.getRecMap(item)
	key := fmt.Sprintf("%v", mp[s.key])
	// Create the record
	return sdk.Util.Source.NewRecordSnapshot(
		opencdc.Position(key),
		map[string]string{
			opencdc.MetadataCollection: s.tableName,
		},
		opencdc.StructuredData{
			s.key: mp[s.key],
		},
		opencdc.StructuredData(mp),
	), nil
}

func (s *SnapshotIterator) Stop() {
	// nothing to stop
}

func (s *SnapshotIterator) getRecMap(item map[string]types.AttributeValue) map[string]interface{} {
	stringMap := make(map[string]interface{})
	for k, v := range item {
		switch v := v.(type) {
		case *types.AttributeValueMemberS:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberN:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberB:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberBOOL:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberSS:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberNS:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberBS:
			stringMap[k] = v.Value
		case *types.AttributeValueMemberM:
			// todo: flatten
			stringMap[k] = v.Value
		case *types.AttributeValueMemberL:
			// todo: flatten
			stringMap[k] = v.Value
		case *types.AttributeValueMemberNULL:
			stringMap[k] = nil
		default:
			stringMap[k] = "<unknown>"
		}
	}
	return stringMap
}

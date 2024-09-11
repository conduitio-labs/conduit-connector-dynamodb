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
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	stypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

// CDCIterator iterates through the table's stream.
type CDCIterator struct {
	tableName          string
	partitionKey       string
	sortKey            string
	streamsClient      *dynamodbstreams.Client
	lastSequenceNumber *string
	cache              chan stypes.Record
	streamArn          string
	shardIterator      *string
	shardIndex         int
	tomb               *tomb.Tomb
	ticker             *time.Ticker
	p                  position.Position
}

// NewCDCIterator initializes a CDCIterator starting from the provided position.
func NewCDCIterator(ctx context.Context, tableName string, pKey string, sKey string, pollingPeriod time.Duration, client *dynamodbstreams.Client, streamArn string, p position.Position) (*CDCIterator, error) {
	c := &CDCIterator{
		tableName:          tableName,
		partitionKey:       pKey,
		sortKey:            sKey,
		streamsClient:      client,
		lastSequenceNumber: nil,
		streamArn:          streamArn,
		tomb:               &tomb.Tomb{},
		cache:              make(chan stypes.Record, 10), // todo size?
		ticker:             time.NewTicker(pollingPeriod),
		p:                  p, // todo position handling when pipeline is restarted
	}
	shardIterator, err := c.getShardIterator(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator: %w", err)
	}
	c.shardIterator = shardIterator

	// start listening to changes
	c.tomb.Go(c.startCDC)

	return c, nil
}

// HasNext returns a boolean that indicates whether the iterator has any objects in the buffer or not.
func (c *CDCIterator) HasNext(_ context.Context) bool {
	return len(c.cache) > 0 || !c.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

// Next returns the next record in the iterator.
func (c *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	var rec stypes.Record
	select {
	case r := <-c.cache:
		rec = r
	case <-c.tomb.Dead():
		return opencdc.Record{}, fmt.Errorf("tomb is dead: %w", c.tomb.Err())
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	}

	return c.getOpenCDCRec(rec)
}

func (c *CDCIterator) Stop() {
	// stop the goRoutine
	c.ticker.Stop()
	c.tomb.Kill(errors.New("cdc iterator is stopped"))
}

// startCDC gets records from the stream using the shardIterators,
// then adds the records to a buffered cache.
func (c *CDCIterator) startCDC() error {
	defer close(c.cache)

	for {
		select {
		case <-c.tomb.Dying():
			return fmt.Errorf("tomb is dying: %w", c.tomb.Err())
		case <-c.ticker.C: // detect changes every polling period
			out, err := c.streamsClient.GetRecords(c.tomb.Context(nil), &dynamodbstreams.GetRecordsInput{ //nolint:staticcheck // SA1012 tomb expects nil
				ShardIterator: c.shardIterator,
			})
			if err != nil {
				return fmt.Errorf("error getting records: %w", err)
			}

			// process the stream records
			for _, record := range out.Records {
				select {
				case c.cache <- record:
					// Successfully sent record
				case <-c.tomb.Dying():
					return fmt.Errorf("tomb is dying: %w", c.tomb.Err())
				}
			}

			// update the shard iterator
			c.shardIterator = out.NextShardIterator

			if c.shardIterator == nil {
				c.shardIterator, err = c.getShardIterator(c.tomb.Context(nil)) //nolint:staticcheck // SA1012 tomb expects nil
				if err != nil {
					return fmt.Errorf("failed to get shard iterator: %w", err)
				}
			}
		}
	}
}

// getShardIterator gets the shard iterator for the stream.
func (c *CDCIterator) getShardIterator(ctx context.Context) (*string, error) {
	// Describe the stream to get the shard ID
	describeStreamOutput, err := c.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}

	// Start from the last shard
	if c.shardIndex == 0 {
		c.shardIndex = len(describeStreamOutput.StreamDescription.Shards) - 1
	} else {
		c.shardIndex++
	}
	shardID := describeStreamOutput.StreamDescription.Shards[c.shardIndex].ShardId

	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(c.streamArn),
		ShardId:           shardID,
		ShardIteratorType: stypes.ShardIteratorTypeLatest,
	}
	// Get the shard iterator
	getShardIteratorOutput, err := c.streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator: %w", err)
	}

	return getShardIteratorOutput.ShardIterator, nil
}

func (c *CDCIterator) getRecMap(item map[string]stypes.AttributeValue) map[string]interface{} { //nolint:dupl // different types
	stringMap := make(map[string]interface{})
	for k, v := range item {
		switch v := v.(type) {
		case *stypes.AttributeValueMemberS:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberN:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberB:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberBOOL:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberSS:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberNS:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberBS:
			stringMap[k] = v.Value
		case *stypes.AttributeValueMemberM:
			stringMap[k] = c.getRecMap(v.Value)
		case *stypes.AttributeValueMemberL:
			// Flatten the list by processing each item
			var list []interface{}
			for _, listItem := range v.Value {
				list = append(list, c.getRecMap(map[string]stypes.AttributeValue{"": listItem})[""])
			}
			stringMap[k] = list
		case *stypes.AttributeValueMemberNULL:
			stringMap[k] = nil
		default:
			stringMap[k] = "<unknown>"
		}
	}
	return stringMap
}

func (c *CDCIterator) getOpenCDCRec(rec stypes.Record) (opencdc.Record, error) {
	newImage := c.getRecMap(rec.Dynamodb.NewImage)
	oldImage := c.getRecMap(rec.Dynamodb.OldImage)

	image := newImage
	// use the old image to get the deleted record's keys
	if rec.EventName == stypes.OperationTypeRemove {
		image = oldImage
	}

	// prepare key and position
	structuredKey := opencdc.StructuredData{
		c.partitionKey: image[c.partitionKey],
	}
	c.p.Key = *rec.Dynamodb.SequenceNumber
	c.p.Type = position.TypeCDC
	if c.sortKey != "" {
		c.p.Key = c.p.Key + "." + fmt.Sprintf("%v", image[c.sortKey])
		structuredKey = opencdc.StructuredData{
			c.partitionKey: image[c.partitionKey],
			c.sortKey:      image[c.sortKey],
		}
	}

	// build the record depending on the event's operation
	switch rec.EventName {
	case stypes.OperationTypeInsert:
		return sdk.Util.Source.NewRecordCreate(
			c.p.ToRecordPosition(),
			map[string]string{
				opencdc.MetadataCollection: c.tableName,
			},
			structuredKey,
			opencdc.StructuredData(newImage),
		), nil
	case stypes.OperationTypeModify:
		return sdk.Util.Source.NewRecordUpdate(
			c.p.ToRecordPosition(),
			map[string]string{
				opencdc.MetadataCollection: c.tableName,
			},
			structuredKey,
			opencdc.StructuredData(oldImage),
			opencdc.StructuredData(newImage),
		), nil
	case stypes.OperationTypeRemove:
		return sdk.Util.Source.NewRecordDelete(
			c.p.ToRecordPosition(),
			map[string]string{
				opencdc.MetadataCollection: c.tableName,
			},
			structuredKey,
			opencdc.StructuredData(oldImage),
		), nil
	default:
		return opencdc.Record{}, fmt.Errorf("unknown operation name: %s", rec.EventName)
	}
}

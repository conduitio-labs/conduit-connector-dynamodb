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
	tableName     string
	partitionKey  string
	sortKey       string
	streamsClient *dynamodbstreams.Client
	cache         chan stypes.Record
	streamArn     string
	shardIterator *string
	shardIndex    int
	tomb          *tomb.Tomb
	ticker        *time.Ticker
	p             position.Position
}

// NewCDCIterator initializes a CDCIterator starting from the provided position.
func NewCDCIterator(ctx context.Context, tableName string, pKey string, sKey string, pollingPeriod time.Duration, client *dynamodbstreams.Client, streamArn string, p position.Position) (*CDCIterator, error) {
	c := &CDCIterator{
		tableName:     tableName,
		partitionKey:  pKey,
		sortKey:       sKey,
		streamsClient: client,
		streamArn:     streamArn,
		tomb:          &tomb.Tomb{},
		cache:         make(chan stypes.Record, 10), // todo size?
		ticker:        time.NewTicker(pollingPeriod),
		p:             p,
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
				// skip older events (this case would only be hit if the pipeline restarts after snapshot and before CDC).
				if record.Dynamodb.ApproximateCreationDateTime != nil && !record.Dynamodb.ApproximateCreationDateTime.After(c.p.Time) {
					continue
				}
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
				c.shardIterator, err = c.moveToNextShard(c.tomb.Context(nil)) //nolint:staticcheck // SA1012 tomb expects nil
				if err != nil {
					return fmt.Errorf("failed to get shard iterator: %w", err)
				}
			}
		}
	}
}

// getShardIterator gets the shard iterator for the stream depending on the position.
// if position is nil, it starts from the last shard with the "LATEST" iterator type.
// if position has a sequence number, it starts from the shard containing that sequence number, with the "AFTER_SEQUENCE_NUMBER" iterator type.
// if the given sequence number was not found (expired) or was at the end of the shard, then we start from the following shard to it, with the "TRIM_HORIZON" iterator type.
func (c *CDCIterator) getShardIterator(ctx context.Context) (*string, error) {
	// describe the stream to get the shard ID.
	describeStreamOutput, err := c.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}

	shards := describeStreamOutput.StreamDescription.Shards
	if len(shards) == 0 {
		return nil, errors.New("no shards found in the stream")
	}

	var selectedShardID string
	var shardIteratorType stypes.ShardIteratorType
	// If position has a sequence number, find the shard containing it.
	if c.p.SequenceNumber != "" {
		for i, shard := range shards {
			// if the sequence number is at the end of the shard (shard was closed after it), then we'll start from the next shard with the "TRIM_HORIZON" iterator type.
			if shard.SequenceNumberRange.EndingSequenceNumber != nil && c.p.SequenceNumber == *shard.SequenceNumberRange.EndingSequenceNumber {
				c.shardIndex = i + 1
				selectedShardID = *shards[c.shardIndex].ShardId // Start from shard following this one.
				shardIteratorType = stypes.ShardIteratorTypeTrimHorizon
				break
			}
			// find the shard contains the position's sequence number, then we'll start from this shard, with the "AFTER_SEQUENCE_NUMBER" iterator type.
			if *shard.SequenceNumberRange.StartingSequenceNumber <= c.p.SequenceNumber &&
				(shard.SequenceNumberRange.EndingSequenceNumber == nil ||
					c.p.SequenceNumber < *shard.SequenceNumberRange.EndingSequenceNumber) {
				c.shardIndex = i
				selectedShardID = *shard.ShardId
				shardIteratorType = stypes.ShardIteratorTypeAfterSequenceNumber
				break
			}
		}

		// if no shard was found containing the sequence number, then start from the beginning of all shards.
		if selectedShardID == "" {
			c.shardIndex = 0
			selectedShardID = *shards[c.shardIndex].ShardId // Start from the first shard
			shardIteratorType = stypes.ShardIteratorTypeTrimHorizon
			sdk.Logger(ctx).Warn().Msg("The given sequence number is expired, connector will start getting events from the beginning of the stream.")
		}
	}
	// if the sequence number is not given.
	if c.p.SequenceNumber == "" {
		if (c.p != position.Position{}) {
			// this is a specific case of which the pipeline was restarted after snapshot and before CDC, so there is no sequence number to start from.
			c.shardIndex = 0
			selectedShardID = *shards[c.shardIndex].ShardId
			shardIteratorType = stypes.ShardIteratorTypeTrimHorizon
		} else {
			// select the latest shard (the last one in the list).
			c.shardIndex = len(shards) - 1
			selectedShardID = *shards[c.shardIndex].ShardId
			shardIteratorType = stypes.ShardIteratorTypeLatest
		}
	}

	// now that we have the shard ID, we can fetch the shard iterator
	return c.getShardIteratorForShard(ctx, selectedShardID, shardIteratorType)
}

// getShardIteratorForShard gets the shard iterator for a specific shard.
func (c *CDCIterator) getShardIteratorForShard(ctx context.Context, shardID string, shardIteratorType stypes.ShardIteratorType) (*string, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(c.streamArn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: shardIteratorType,
	}
	// continue from a specific position.
	if shardIteratorType == stypes.ShardIteratorTypeAfterSequenceNumber {
		input.SequenceNumber = aws.String(c.p.SequenceNumber)
	}

	// get the shard iterator.
	getShardIteratorOutput, err := c.streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator for shard %s: %w", shardID, err)
	}

	return getShardIteratorOutput.ShardIterator, nil
}

// moveToNextShard used to get the iterator of the shard that follows the current one after it was closed.
func (c *CDCIterator) moveToNextShard(ctx context.Context) (*string, error) {
	// describe the stream to get the shard details.
	describeStreamOutput, err := c.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}

	shards := describeStreamOutput.StreamDescription.Shards

	// move to the next shard if the current one is closed
	if c.shardIndex+1 < len(shards) {
		c.shardIndex++
		nextShard := shards[c.shardIndex]
		// get shard iterator for the new shard
		return c.getShardIteratorForShard(ctx, *nextShard.ShardId, stypes.ShardIteratorTypeTrimHorizon)
	}
	return nil, errors.New("no more shards available")
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
	if c.sortKey != "" {
		structuredKey = opencdc.StructuredData{
			c.partitionKey: image[c.partitionKey],
			c.sortKey:      image[c.sortKey],
		}
	}
	pos := position.Position{
		IteratorType:   position.TypeCDC,
		SequenceNumber: *rec.Dynamodb.SequenceNumber,
		Time:           time.Now(),
	}
	cdcPos, err := pos.ToRecordPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to build record's CDC position: %w", err)
	}

	// build the record depending on the event's operation
	switch rec.EventName {
	case stypes.OperationTypeInsert:
		return sdk.Util.Source.NewRecordCreate(
			cdcPos,
			map[string]string{
				opencdc.MetadataCollection: c.tableName,
			},
			structuredKey,
			opencdc.StructuredData(newImage),
		), nil
	case stypes.OperationTypeModify:
		return sdk.Util.Source.NewRecordUpdate(
			cdcPos,
			map[string]string{
				opencdc.MetadataCollection: c.tableName,
			},
			structuredKey,
			opencdc.StructuredData(oldImage),
			opencdc.StructuredData(newImage),
		), nil
	case stypes.OperationTypeRemove:
		return sdk.Util.Source.NewRecordDelete(
			cdcPos,
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

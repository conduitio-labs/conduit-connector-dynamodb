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
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	stypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

type CDCIterator struct {
	tableName     string
	partitionKey  string
	sortKey       string
	streamsClient *dynamodbstreams.Client
	cache         []recordWithTimestamp
	cacheLock     sync.Mutex
	cacheCond     *sync.Cond
	streamArn     string
	tomb          *tomb.Tomb
	p             position.Position
	shards        map[string]*shardProcessor
}

type recordWithTimestamp struct {
	Record stypes.Record
	Time   time.Time
}

type shardProcessor struct {
	shardID       string
	shardIterator *string
	tomb          *tomb.Tomb
}

func NewCDCIterator(ctx context.Context, tableName string, pKey string, sKey string, client *dynamodbstreams.Client, streamArn string, p position.Position) (*CDCIterator, error) {
	c := &CDCIterator{
		tableName:     tableName,
		partitionKey:  pKey,
		sortKey:       sKey,
		streamsClient: client,
		streamArn:     streamArn,
		tomb:          &tomb.Tomb{},
		p:             p,
		shards:        make(map[string]*shardProcessor),
	}
	c.cacheCond = sync.NewCond(&c.cacheLock)

	// start listening to changes
	c.tomb.Go(func() error {
		return c.startCDC(ctx)
	})

	return c, nil
}

func (c *CDCIterator) HasNext(_ context.Context) bool {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	return len(c.cache) > 0 || !c.tomb.Alive()
}

func (c *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	if len(c.cache) == 0 {
		select {
		case <-c.tomb.Dying():
			return opencdc.Record{}, c.tomb.Err()
		case <-ctx.Done():
			return opencdc.Record{}, ctx.Err()
		default:
			return opencdc.Record{}, sdk.ErrBackoffRetry
		}
	}
	// get the oldest record
	c.cacheLock.Lock()
	recWithTimestamp := c.cache[0]
	c.cache = c.cache[1:]
	c.cacheLock.Unlock()

	return c.getOpenCDCRec(recWithTimestamp.Record)
}

func (c *CDCIterator) Stop() {
	c.tomb.Kill(errors.New("cdc iterator is stopped"))
	c.cacheCond.Broadcast()
}

func (c *CDCIterator) startCDC(ctx context.Context) error {
	defer func() {
		c.cacheCond.Broadcast()
	}()

	// start a goroutine to discover shards
	c.tomb.Go(func() error {
		return c.discoverShards(ctx)
	})

	// wait for all shard processors to finish
	err := c.tomb.Wait()
	if err != nil {
		return fmt.Errorf("tomb wait: %w", err)
	}

	return nil
}

func (c *CDCIterator) discoverShards(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.tomb.Dying():
			c.stopAllShards()
			return nil
		case <-ticker.C:
			// discover shards
			err := c.updateShards()
			if err != nil {
				sdk.Logger(context.Background()).Error().Err(err).Msg("Failed to update shards")
			}
		case <-ctx.Done():
			c.stopAllShards()
			return ctx.Err()
		}
	}
}

func (c *CDCIterator) updateShards() error {
	ctx := context.Background()
	describeStreamOutput, err := c.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.streamArn),
	})
	if err != nil {
		return fmt.Errorf("failed to describe stream: %w", err)
	}

	// determine shards to process
	var shardsToProcess []stypes.Shard
	if c.p.SequenceNumber == "" {
		// if no sequence number, process all shards
		shardsToProcess = describeStreamOutput.StreamDescription.Shards
	}
	if c.p.SequenceNumber != "" {
		// find the shard containing the sequence number, start processing from it.
		foundShard := false
		for _, shard := range describeStreamOutput.StreamDescription.Shards {
			startSeqNum := *shard.SequenceNumberRange.StartingSequenceNumber
			endSeqNum := "" // open ended shard
			if shard.SequenceNumberRange.EndingSequenceNumber != nil {
				endSeqNum = *shard.SequenceNumberRange.EndingSequenceNumber
			}

			if foundShard {
				// found the shard, process all shards next.
				shardsToProcess = append(shardsToProcess, shard)
			}
			// check if the sequence number is in this shard
			if isSequenceNumberInRange(c.p.SequenceNumber, startSeqNum, endSeqNum) {
				foundShard = true
				shardsToProcess = append(shardsToProcess, shard)
			}
		}
		if !foundShard {
			return fmt.Errorf("sequence number %s not found in any shard", c.p.SequenceNumber)
		}
	}

	// update shards map
	for _, shard := range shardsToProcess {
		shardID := *shard.ShardId
		if _, exists := c.shards[shardID]; !exists {
			// start processing this shard
			shardProcessor, err := c.startShardProcessor(ctx, shard, c.p.SequenceNumber)
			if err != nil {
				sdk.Logger(ctx).Error().Err(err).Msgf("Failed to start shard processor for shard %s", shardID)
				continue
			}
			c.shards[shardID] = shardProcessor
			// reset SequenceNumber so that next shards uses TrimHorizon
			c.p.SequenceNumber = ""
		}
	}
	return nil
}

func (c *CDCIterator) startShardProcessor(ctx context.Context, shard stypes.Shard, sequenceNumber string) (*shardProcessor, error) {
	shardID := *shard.ShardId
	var shardIteratorType stypes.ShardIteratorType
	var startingSequenceNumber *string

	if sequenceNumber != "" {
		// this is the shard containing the sequence number
		shardIteratorType = stypes.ShardIteratorTypeAfterSequenceNumber
		startingSequenceNumber = aws.String(sequenceNumber)
	} else {
		// for other shards, use TrimHorizon to read from the beginning
		shardIteratorType = stypes.ShardIteratorTypeTrimHorizon
	}

	input := &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(c.streamArn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: shardIteratorType,
		SequenceNumber:    startingSequenceNumber,
	}

	getShardIteratorOutput, err := c.streamsClient.GetShardIterator(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator for shard %s: %w", shardID, err)
	}

	shardProcessor := &shardProcessor{
		shardID:       shardID,
		shardIterator: getShardIteratorOutput.ShardIterator,
		tomb:          &tomb.Tomb{},
	}

	// start processing this shard
	shardProcessor.tomb.Go(func() error {
		return c.processShard(ctx, shardProcessor)
	})

	return shardProcessor, nil
}

func isSequenceNumberInRange(targetSeqNum, startSeqNum, endSeqNum string) bool {
	target := new(big.Int)
	start := new(big.Int)
	end := new(big.Int)

	target.SetString(targetSeqNum, 10)
	start.SetString(startSeqNum, 10)

	if endSeqNum != "" {
		end.SetString(endSeqNum, 10)
		return target.Cmp(start) >= 0 && target.Cmp(end) < 0
	}
	return target.Cmp(start) >= 0
}

func (c *CDCIterator) processShard(ctx context.Context, s *shardProcessor) error {
	for {
		select {
		case <-c.tomb.Dying():
			return nil
		case <-s.tomb.Dying():
			return nil
		default:
			out, err := c.streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: s.shardIterator,
			})
			if err != nil {
				return fmt.Errorf("error getting records from shard %s: %w", s.shardID, err)
			}

			// process records
			changed := false
			for _, record := range out.Records {
				if record.Dynamodb.ApproximateCreationDateTime != nil && !record.Dynamodb.ApproximateCreationDateTime.After(c.p.Time) {
					continue
				}
				if !changed {
					c.cacheLock.Lock()
					changed = true
				}
				recWithTimestamp := recordWithTimestamp{
					Record: record,
					Time:   *record.Dynamodb.ApproximateCreationDateTime,
				}
				// add to buffer
				c.cache = append(c.cache, recWithTimestamp)
			}

			// sort the buffer after all records were added
			if changed {
				sort.Slice(c.cache, func(i, j int) bool {
					return c.cache[i].Time.Before(c.cache[j].Time)
				})
				c.cacheLock.Unlock()
				c.cacheCond.Signal()
			}

			s.shardIterator = out.NextShardIterator
			// check if shard is closed
			if s.shardIterator == nil {
				return nil
			}

			// sleep to not cause CPU overhead
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *CDCIterator) stopAllShards() {
	for _, shard := range c.shards {
		shard.tomb.Kill(nil)
	}
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
	if rec.EventName == stypes.OperationTypeRemove {
		image = oldImage
	}
	structuredKey := opencdc.StructuredData{
		c.partitionKey: image[c.partitionKey],
	}
	if c.sortKey != "" {
		structuredKey[c.sortKey] = image[c.sortKey]
	}
	pos := position.Position{
		IteratorType:   position.TypeCDC,
		SequenceNumber: *rec.Dynamodb.SequenceNumber,
		Time:           *rec.Dynamodb.ApproximateCreationDateTime,
	}
	cdcPos, err := pos.ToRecordPosition()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to build record's CDC position: %w", err)
	}

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

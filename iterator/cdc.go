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
	streamArn     string
	tomb          *tomb.Tomb
	shards        map[string]*shardProcessor
	lastPosition  position.Position
	pollingPeriod time.Duration
}

type recordWithTimestamp struct {
	Record  stypes.Record
	Time    time.Time
	ShardID string
}

type shardProcessor struct {
	shardID       string
	shardIterator *string
	tomb          *tomb.Tomb
}

func NewCDCIterator(ctx context.Context, tableName string, pKey string, sKey string, client *dynamodbstreams.Client, streamArn string, pollingPeriod time.Duration, p position.Position) (*CDCIterator, error) {
	c := &CDCIterator{
		tableName:     tableName,
		partitionKey:  pKey,
		sortKey:       sKey,
		streamsClient: client,
		streamArn:     streamArn,
		tomb:          &tomb.Tomb{},
		shards:        make(map[string]*shardProcessor),
		lastPosition:  p,
		pollingPeriod: pollingPeriod,
	}

	// start listening to changes
	c.tomb, ctx = tomb.WithContext(ctx)
	c.tomb.Go(func() error {
		return c.startCDC(ctx)
	})

	return c, nil
}

func (c *CDCIterator) HasNext(_ context.Context) bool {
	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()
	return len(c.cache) > 0 || !c.tomb.Alive() // if tomb is dead we return true so caller will fetch error with Next
}

func (c *CDCIterator) Next(ctx context.Context) (opencdc.Record, error) {
	if len(c.cache) == 0 {
		select {
		case <-c.tomb.Dead():
			return opencdc.Record{}, fmt.Errorf("tomb closed: %w", c.tomb.Err())
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

	return c.getOpenCDCRec(recWithTimestamp)
}

func (c *CDCIterator) Stop() {
	c.tomb.Kill(errors.New("cdc iterator is stopped"))
}

func (c *CDCIterator) startCDC(ctx context.Context) error {
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
	ticker := time.NewTicker(c.pollingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-c.tomb.Dying():
			c.stopAllShards()
			return nil
		case <-ticker.C:
			// discover shards
			err := c.updateShards(ctx)
			if err != nil {
				sdk.Logger(ctx).Error().Err(err).Msg("Failed to update shards")
			}
		case <-ctx.Done():
			c.stopAllShards()
			return ctx.Err()
		}
	}
}

func (c *CDCIterator) updateShards(ctx context.Context) error {
	describeStreamOutput, err := c.streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.streamArn),
	})
	if err != nil {
		return fmt.Errorf("failed to describe stream: %w", err)
	}

	// determine shards to process
	shardsToProcess := describeStreamOutput.StreamDescription.Shards
	for _, shard := range shardsToProcess {
		shardID := *shard.ShardId
		if _, exists := c.shards[shardID]; !exists {
			// start processing this shard
			seqNum, ok := c.lastPosition.SequenceNumberMap[shardID]
			if !ok {
				seqNum = "" // to use trim horizon, which will start reading from the beginning of the shard.
			}
			shardProcessor, err := c.startShardProcessor(ctx, shard, seqNum)
			if err != nil {
				sdk.Logger(ctx).Error().Err(err).Msgf("Failed to start shard processor for shard %s", shardID)
				continue
			}
			c.shards[shardID] = shardProcessor
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

func (c *CDCIterator) processShard(ctx context.Context, s *shardProcessor) error {
	defer func() {
		// clenaup the shard from the SequenceNumberMap when shard is closed.
		c.cacheLock.Lock()
		delete(c.lastPosition.SequenceNumberMap, s.shardID)
		c.cacheLock.Unlock()
	}()
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
				if c.lastPosition.AfterSnapshot {
					if record.Dynamodb.ApproximateCreationDateTime != nil && !record.Dynamodb.ApproximateCreationDateTime.After(c.lastPosition.Time) {
						continue
					}
				}
				recWithTimestamp := recordWithTimestamp{
					Record:  record,
					ShardID: s.shardID,
					Time:    *record.Dynamodb.ApproximateCreationDateTime,
				}
				if !changed {
					c.cacheLock.Lock()
					changed = true
				}
				// add to buffer
				c.cache = append(c.cache, recWithTimestamp)
			}
			if changed {
				sort.Slice(c.cache, func(i, j int) bool {
					return c.cache[i].Time.Before(c.cache[j].Time)
				})
				c.cacheLock.Unlock()
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

func (c *CDCIterator) getOpenCDCRec(recWithTS recordWithTimestamp) (opencdc.Record, error) {
	rec := recWithTS.Record

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

	// lock position
	c.lastPosition.SequenceNumberMap[recWithTS.ShardID] = *rec.Dynamodb.SequenceNumber
	cdcPos, err := c.lastPosition.ToRecordPosition()
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

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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type CombinedIterator struct {
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	tableName     string
	partitionKey  string
	sortKey       string
	client        *dynamodb.Client
	streamsClient *dynamodbstreams.Client
	streamArn     string
	pollingPeriod time.Duration
}

func NewCombinedIterator(
	ctx context.Context,
	tableName, pKey, sKey string,
	pollingPeriod time.Duration,
	client *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	streamArn string,
	p position.Position,
) (*CombinedIterator, error) {
	var err error
	c := &CombinedIterator{
		tableName:     tableName,
		partitionKey:  pKey,
		sortKey:       sKey,
		pollingPeriod: pollingPeriod,
		client:        client,
		streamsClient: streamsClient,
		streamArn:     streamArn,
	}

	switch p.IteratorType {
	case position.TypeSnapshot:
		if len(p.PartitionKey) != 0 {
			sdk.Logger(ctx).
				Warn().
				Msg("previous snapshot did not complete successfully. snapshot will be restarted for consistency.")
		}
		p = position.Position{} // always start snapshot from the beginning, so position is nil
		c.snapshotIterator, err = NewSnapshotIterator(tableName, pKey, sKey, client, p)
		if err != nil {
			return nil, fmt.Errorf("could not create the snapshot iterator: %w", err)
		}
		// start listening for changes while snapshot is running
		c.cdcIterator, err = NewCDCIterator(ctx, tableName, pKey, sKey, pollingPeriod, streamsClient, streamArn, position.Position{})
		if err != nil {
			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}
	case position.TypeCDC:
		c.cdcIterator, err = NewCDCIterator(ctx, tableName, pKey, sKey, pollingPeriod, streamsClient, streamArn, p)
		if err != nil {
			return nil, fmt.Errorf("could not create the CDC iterator: %w", err)
		}
	default:
		return nil, fmt.Errorf("invalid position type (%d)", p.IteratorType)
	}
	return c, nil
}

func (c *CombinedIterator) HasNext(ctx context.Context) bool {
	switch {
	case c.snapshotIterator != nil:
		return c.snapshotIterator.HasNext(ctx)
	case c.cdcIterator != nil:
		return c.cdcIterator.HasNext(ctx)
	default:
		return false
	}
}

func (c *CombinedIterator) Next(ctx context.Context) (opencdc.Record, error) {
	switch {
	case c.snapshotIterator != nil:
		r, err := c.snapshotIterator.Next(ctx)
		if err != nil {
			return opencdc.Record{}, err
		}
		if !c.snapshotIterator.HasNext(ctx) {
			c.snapshotIterator = nil
			// change the last snapshot record's position to CDC
			r.Position, err = position.ConvertToCDCPosition(r.Position)
			if err != nil {
				return opencdc.Record{}, fmt.Errorf("error converting position to CDC: %w", err)
			}
		}
		return r, nil

	case c.cdcIterator != nil:
		return c.cdcIterator.Next(ctx)
	default:
		return opencdc.Record{}, errors.New("no initialized iterator")
	}
}

func (c *CombinedIterator) Stop() {
	if c.cdcIterator != nil {
		c.cdcIterator.Stop()
	}
}

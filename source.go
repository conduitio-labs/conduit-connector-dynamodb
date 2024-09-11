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

package dynamodb

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/conduitio-labs/conduit-connector-dynamodb/iterator"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config           SourceConfig
	dynamoDBClient   *dynamodb.Client
	streamsClient    *dynamodbstreams.Client
	lastPositionRead opencdc.Position
	streamArn        string
	iterator         Iterator
}

type SourceConfig struct {
	// Table is the DynamoDB table name to pull data from.
	Table string `json:"table" validate:"required"`
	// AWS region.
	AWSRegion string `json:"aws.region" validate:"required"`
	// AWS access key id.
	AWSAccessKeyID string `json:"aws.accessKeyId" validate:"required"`
	// AWS secret access key.
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
	// polling period for the CDC mode, formatted as a time.Duration string.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"1s"`
	// skipSnapshot determines weather to skip the snapshot or not.
	SkipSnapshot bool `json:"skipSnapshot" default:"false"`
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() cconfig.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg cconfig.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring DynamoDB Source...")
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(ctx context.Context, pos opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening DynamoDB Source...")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s.config.AWSRegion),
		config.WithCredentialsProvider(aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(s.config.AWSAccessKeyID, s.config.AWSSecretAccessKey, ""))),
	)
	if err != nil {
		return fmt.Errorf("error creating AWS session: %w", err)
	}

	s.dynamoDBClient = dynamodb.NewFromConfig(cfg)
	s.streamsClient = dynamodbstreams.NewFromConfig(cfg)
	s.lastPositionRead = pos

	partitionKey, sortKey, err := s.getKeyNamesFromTable(ctx)
	if err != nil {
		return fmt.Errorf("error getting key names from table: %w", err)
	}
	err = s.prepareStream(ctx)
	if err != nil {
		return err
	}

	p, err := position.ParseRecordPosition(pos)
	if err != nil {
		return fmt.Errorf("error parssing position: %w", err)
	}

	// create the needed iterator
	var itr Iterator
	if s.config.SkipSnapshot {
		itr, err = iterator.NewCDCIterator(ctx, s.config.Table, partitionKey, sortKey, s.config.PollingPeriod, s.streamsClient, s.streamArn, p)
		if err != nil {
			return fmt.Errorf("error creating CDC iterator: %w", err)
		}
	} else {
		itr, err = iterator.NewCombinedIterator(ctx, s.config.Table, partitionKey, sortKey, s.config.PollingPeriod, s.dynamoDBClient, s.streamsClient, s.streamArn, p)
		if err != nil {
			return fmt.Errorf("error creating combined iterator: %w", err)
		}
	}
	s.iterator = itr
	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Info().Msg("Reading records from DynamoDB...")

	if !s.iterator.HasNext(ctx) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error getting next record: %w", err)
	}
	return r, nil
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("Acknowledged position")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down DynamoDB Source...")
	return nil
}

func describeTable(ctx context.Context, client *dynamodb.Client, tableName string) (*dynamodb.DescribeTableOutput, error) {
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}
	desc, err := client.DescribeTable(ctx, describeTableInput)
	if err != nil {
		return nil, fmt.Errorf("error describing table: %w", err)
	}
	return desc, nil
}

func enableStream(ctx context.Context, client *dynamodb.Client, tableName string) error {
	updateTableInput := &dynamodb.UpdateTableInput{
		TableName: &tableName,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	}
	_, err := client.UpdateTable(ctx, updateTableInput)
	return fmt.Errorf("failed to enable stream on DynamoDB table: %w", err)
}

func (s *Source) prepareStream(ctx context.Context) error {
	// Describe the table to get Stream ARN
	out, err := describeTable(ctx, s.dynamoDBClient, s.config.Table)
	if err != nil {
		return err
	}

	if out.Table.LatestStreamArn != nil && out.Table.StreamSpecification != nil && aws.ToBool(out.Table.StreamSpecification.StreamEnabled) {
		sdk.Logger(ctx).Info().Str("LatestStreamArn", *out.Table.LatestStreamArn).Msg("LatestStreamArn found.")
		s.streamArn = *out.Table.LatestStreamArn
		return nil
	}

	sdk.Logger(ctx).Info().Msg("No stream enabled. Enabling stream...")

	// Enable stream if not present
	if err = enableStream(ctx, s.dynamoDBClient, s.config.Table); err != nil {
		return err
	}

	// Describe the table again to get the LatestStreamArn
	out, err = s.waitForStreamToBeEnabled(ctx)
	if err != nil {
		return err
	}

	if out.Table.LatestStreamArn == nil {
		return fmt.Errorf("stream was not enabled successfully")
	}

	sdk.Logger(ctx).Info().Str("LatestStreamArn", *out.Table.LatestStreamArn).Msg("Stream enabled successfully.")
	s.streamArn = *out.Table.LatestStreamArn
	return nil
}

// todo: this method doesn't work as expected, stream is created but not ready to fetch data from yet.
func (s *Source) waitForStreamToBeEnabled(ctx context.Context) (*dynamodb.DescribeTableOutput, error) {
	sdk.Logger(ctx).Info().Msg("waiting for stream to be enabled...")
	for {
		// Describe the table to check stream status
		describeTableInput := &dynamodb.DescribeTableInput{
			TableName: aws.String(s.config.Table),
		}

		describeTableOutput, err := s.dynamoDBClient.DescribeTable(ctx, describeTableInput)
		if err != nil {
			return nil, fmt.Errorf("error describing DynamoDB table: %w", err)
		}

		// Check if the stream is enabled
		if describeTableOutput.Table.StreamSpecification != nil && aws.ToBool(describeTableOutput.Table.StreamSpecification.StreamEnabled) {
			return describeTableOutput, nil
		}
		// Wait before checking again
		time.Sleep(2 * time.Second)
	}
}

func (s *Source) getKeyNamesFromTable(ctx context.Context) (partitionKey string, sortKey string, err error) {
	out, err := describeTable(ctx, s.dynamoDBClient, s.config.Table)
	if err != nil {
		return "", "", fmt.Errorf("error describing table: %w", err)
	}

	// Iterate over the key schema to find the key names
	for _, keySchemaElement := range out.Table.KeySchema {
		switch keySchemaElement.KeyType {
		case types.KeyTypeHash:
			partitionKey = *keySchemaElement.AttributeName
		case types.KeyTypeRange:
			sortKey = *keySchemaElement.AttributeName
		}
	}

	return partitionKey, sortKey, nil
}

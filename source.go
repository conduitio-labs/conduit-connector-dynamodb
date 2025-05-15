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

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/conduitio-labs/conduit-connector-dynamodb/iterator"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config         SourceConfig
	dynamoDBClient *dynamodb.Client
	streamsClient  *dynamodbstreams.Client
	streamArn      string
	iterator       Iterator
}

type SourceConfig struct {
	sdk.DefaultSourceMiddleware
	Config

	// Discovery polling period for the CDC mode of how often to check for new shards in the DynamoDB Stream, formatted as a time.Duration string.
	DiscoveryPollingPeriod time.Duration `json:"discoveryPollingPeriod" default:"10s"`
	// Records polling period for the CDC mode of how often to get new records from a shard, formatted as a time.Duration string.
	RecordsPollingPeriod time.Duration `json:"recordsPollingPeriod" default:"1s"`
	// SkipSnapshot determines weather to skip the snapshot or not.
	SkipSnapshot bool `json:"skipSnapshot" default:"false"`
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, pos opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening DynamoDB Source...")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s.config.AWSRegion),
		config.WithCredentialsProvider(aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(s.config.AWSAccessKeyID, s.config.AWSSecretAccessKey, s.config.AWSSessionToken))),
	)
	if err != nil {
		return fmt.Errorf("could not load AWS config: %w", err)
	}

	// Set the endpoint if provided for testing
	if s.config.AWSURL != "" {
		s.dynamoDBClient = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.EndpointResolverV2 = staticResolver{
				BaseURL: s.config.AWSURL,
			}
		})
		s.streamsClient = dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
			o.EndpointResolverV2 = staticStreamResolver{
				BaseURL: s.config.AWSURL,
			}
		})
	} else {
		s.dynamoDBClient = dynamodb.NewFromConfig(cfg)
		s.streamsClient = dynamodbstreams.NewFromConfig(cfg)
	}

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
		return fmt.Errorf("error parsing position: %w", err)
	}

	// Create the needed iterator
	var itr Iterator
	if s.config.SkipSnapshot {
		itr, err = iterator.NewCDCIterator(ctx, s.config.Table, partitionKey, sortKey, s.streamsClient, s.streamArn, s.config.DiscoveryPollingPeriod, s.config.RecordsPollingPeriod, p)
		if err != nil {
			return fmt.Errorf("error creating CDC iterator: %w", err)
		}
	} else {
		itr, err = iterator.NewCombinedIterator(ctx, s.config.Table, partitionKey, sortKey, s.config.DiscoveryPollingPeriod, s.config.RecordsPollingPeriod, s.dynamoDBClient, s.streamsClient, s.streamArn, p)
		if err != nil {
			return fmt.Errorf("error creating combined iterator: %w", err)
		}
	}
	s.iterator = itr
	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Trace().Msg("Reading records from DynamoDB...")

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
	if err != nil {
		return fmt.Errorf("failed to enable stream on DynamoDB table: %w", err)
	}
	return nil
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
		return errors.New("stream was not enabled successfully")
	}

	sdk.Logger(ctx).Info().Str("LatestStreamArn", *out.Table.LatestStreamArn).Msg("Stream enabled successfully.")
	s.streamArn = *out.Table.LatestStreamArn
	return nil
}

func (s *Source) waitForStreamToBeEnabled(ctx context.Context) (*dynamodb.DescribeTableOutput, error) {
	sdk.Logger(ctx).Info().Msg("waiting for stream to be enabled...")
	for {
		// Wait before checking
		time.Sleep(5 * time.Second)
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

// staticResolver used to connect to a DynamoDB URL, for tests.
type staticResolver struct {
	BaseURL string
}

func (s staticResolver) ResolveEndpoint(_ context.Context, _ dynamodb.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(s.BaseURL)
	if err != nil {
		return smithyendpoints.Endpoint{}, fmt.Errorf("invalid URL: %w", err)
	}

	return smithyendpoints.Endpoint{URI: *u}, nil
}

// staticStreamResolver used to connect to a DynamoDB URL, for tests.
type staticStreamResolver struct {
	BaseURL string
}

func (s staticStreamResolver) ResolveEndpoint(_ context.Context, _ dynamodbstreams.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(s.BaseURL)
	if err != nil {
		return smithyendpoints.Endpoint{}, fmt.Errorf("invalid URL: %w", err)
	}

	return smithyendpoints.Endpoint{URI: *u}, nil
}

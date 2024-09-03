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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	iterator "github.com/conduitio-labs/conduit-connector-dynamodb/iterators"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
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
	iterator         *iterator.CombinedIterator
}

type SourceConfig struct {
	Table string `json:"table" validate:"required"`
	// todo get key from table

	Key string `json:"key" validate:"required"`
	// todo add sortKey?

	AWSRegion          string `json:"aws.region" validate:"required"`
	AWSAccessKeyID     string `json:"aws.accessKeyId" validate:"required"`
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
}

type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (opencdc.Record, error)
	Stop()
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(
		&Source{},
		sdk.DefaultSourceMiddleware(
			sdk.SourceWithSchemaExtractionConfig{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		)...,
	)
}

func (s *Source) Parameters() cconfig.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg cconfig.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring DynamoDB Source...")
	var sourceConfig SourceConfig
	err := sdk.Util.ParseConfig(ctx, cfg, &sourceConfig, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	s.config = sourceConfig

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

	err = s.prepareStream(ctx)
	if err != nil {
		return err
	}

	p, err := position.ParseRecordPosition(pos)
	if err != nil {
		return err
	}
	s.iterator, err = iterator.NewCombinedIterator(ctx, s.config.Table, s.config.Key, s.dynamoDBClient, s.streamsClient, s.streamArn, p)
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Info().Msg("Reading records from DynamoDB...")

	if !s.iterator.HasNext(ctx) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
	r, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, err
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
	return client.DescribeTable(ctx, describeTableInput)
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
	return err
}

func (s *Source) prepareStream(ctx context.Context) error {
	// Describe the table to get Stream ARN
	out, err := describeTable(ctx, s.dynamoDBClient, s.config.Table)
	if err != nil {
		return fmt.Errorf("error describing table: %w", err)
	}
	streamEnabled := *out.Table.StreamSpecification.StreamEnabled

	if out.Table.LatestStreamArn != nil && streamEnabled {
		sdk.Logger(ctx).Info().Str("LatestStreamArn", *out.Table.LatestStreamArn).Msg("LatestStreamArn found.")
		s.streamArn = *out.Table.LatestStreamArn
		return nil
	}

	sdk.Logger(ctx).Info().Msg("No stream enabled. Enabling stream...")

	// Enable stream if not present
	if err := enableStream(ctx, s.dynamoDBClient, s.config.Table); err != nil {
		return fmt.Errorf("failed to enable stream on DynamoDB table: %w", err)
	}

	// Describe the table again to get the LatestStreamArn
	out, err = describeTable(ctx, s.dynamoDBClient, s.config.Table)
	if err != nil {
		return fmt.Errorf("failed to describe DynamoDB table after enabling stream: %w", err)
	}

	if out.Table.LatestStreamArn == nil {
		return fmt.Errorf("stream was not enabled successfully")
	}

	sdk.Logger(ctx).Info().Str("LatestStreamArn", *out.Table.LatestStreamArn).Msg("Stream enabled successfully.")
	s.streamArn = *out.Table.LatestStreamArn
	return nil
}

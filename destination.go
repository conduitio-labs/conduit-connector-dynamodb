// Copyright © 2025 Meroxa, Inc.
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
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config         DestinationConfig
	dynamoDBClient *dynamodb.Client
}

type DestinationConfig struct {
	sdk.DefaultDestinationMiddleware

	Config
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening DynamoDB Source...")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(d.config.AWSRegion),
		config.WithCredentialsProvider(aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(d.config.AWSAccessKeyID, d.config.AWSSecretAccessKey, ""))),
	)
	if err != nil {
		return fmt.Errorf("could not load AWS config: %w", err)
	}

	// Set the endpoint if provided for testing
	if d.config.AWSURL != "" {
		d.dynamoDBClient = dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.EndpointResolverV2 = staticResolver{
				BaseURL: d.config.AWSURL,
			}
		})
	} else {
		d.dynamoDBClient = dynamodb.NewFromConfig(cfg)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	for i, record := range records {
		var err error
		switch record.Operation {
		case opencdc.OperationCreate, opencdc.OperationSnapshot, opencdc.OperationUpdate:
			err = d.handleInsert(ctx, record)
		case opencdc.OperationDelete:
			err = d.handleDelete(ctx, record)
		default:
			return i, fmt.Errorf("invalid operation %q", record.Operation)
		}
		if err != nil {
			return i, err
		}
	}
	return len(records), nil
}

// handleInsert inserts/updates an item into the DynamoDB table.
func (d *Destination) handleInsert(ctx context.Context, record opencdc.Record) error {
	item, err := recordToDynamoDBItem(record)
	if err != nil {
		return fmt.Errorf("failed to convert record to DynamoDB item: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.config.Table),
		Item:      item,
	}

	_, err = d.dynamoDBClient.PutItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put item into DynamoDB: %w", err)
	}
	return nil
}

// handleDelete deletes an item from the DynamoDB table.
func (d *Destination) handleDelete(ctx context.Context, record opencdc.Record) error {
	key, err := extractKey(record)
	if err != nil {
		return fmt.Errorf("failed to extract key from record: %w", err)
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(d.config.Table),
		Key:       key,
	}

	_, err = d.dynamoDBClient.DeleteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete item from DynamoDB: %w", err)
	}
	return nil
}

func extractKey(record opencdc.Record) (map[string]types.AttributeValue, error) {
	keyData, ok := record.Key.(opencdc.StructuredData)
	if !ok {
		return nil, fmt.Errorf("record key is not structured data")
	}

	key := make(map[string]types.AttributeValue)
	for k, v := range keyData {
		attrValue, err := convertToAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert key field %s: %w", k, err)
		}
		key[k] = attrValue
	}
	return key, nil
}

// recordToDynamoDBItem converts an opencdc.Record to a DynamoDB attribute value map.
func recordToDynamoDBItem(record opencdc.Record) (map[string]types.AttributeValue, error) {
	// ensure the payload is structured data
	data, ok := record.Payload.After.(opencdc.StructuredData)
	if !ok {
		return nil, fmt.Errorf("record payload is not structured data")
	}

	// Convert the structured data to a DynamoDB attribute value map
	item := make(map[string]types.AttributeValue)
	for k, v := range data {
		attrValue, err := convertToAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", k, err)
		}
		item[k] = attrValue
	}
	return item, nil
}

// convertToAttributeValue converts interface{} to a DynamoDB AttributeValue.
func convertToAttributeValue(value interface{}) (types.AttributeValue, error) {
	switch v := value.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: v}, nil
	case int, int32, int64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", v)}, nil
	case float32, float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", v)}, nil
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}, nil
	case []byte:
		return &types.AttributeValueMemberB{Value: v}, nil
	case []interface{}:
		listAttr := make([]types.AttributeValue, len(v))
		for i, item := range v {
			attrValue, err := convertToAttributeValue(item)
			if err != nil {
				return nil, err
			}
			listAttr[i] = attrValue
		}
		return &types.AttributeValueMemberL{Value: listAttr}, nil
	case map[string]interface{}:
		mapAttr := make(map[string]types.AttributeValue)
		for key, val := range v {
			attrValue, err := convertToAttributeValue(val)
			if err != nil {
				return nil, err
			}
			mapAttr[key] = attrValue
		}
		return &types.AttributeValueMemberM{Value: mapAttr}, nil
	default:
		return nil, fmt.Errorf("unsupported attribute value type: %T", v)
	}
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down DynamoDB Destination...")
	return nil
}

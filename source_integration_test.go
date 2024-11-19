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
	"os"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/conduitio-labs/conduit-connector-dynamodb/position"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

var (
	PartitionKey = "pkey"
	SortKey      = "skey"
)

// Records is a slice of opencdc.Record, that can be sorted by the sort key under record.Key["skey"].
type Records []opencdc.Record

// Implementing the sort interface.
func (r Records) Len() int {
	return len(r)
}

func (r Records) Less(i, j int) bool {
	// Ensure that both records have a sort key
	sortKeyI, okI := r[i].Key.(opencdc.StructuredData)[SortKey].(string)
	sortKeyJ, okJ := r[j].Key.(opencdc.StructuredData)[SortKey].(string)

	// If either key is not present or not a string
	if !okI || !okJ {
		return false
	}

	// Compare the sort key values
	return sortKeyI < sortKeyJ
}

func (r Records) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func TestSource_SuccessfulSnapshot(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := prepareIntegrationTest(ctx, t)

	testTable := cfg[SourceConfigTable]
	source := &Source{}
	err := source.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// insert 5 rows
	err = insertRecord(ctx, client, testTable, 0, 5)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	var got Records
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		rec, err := source.Read(timeoutCtx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			break
		}
		is.NoErr(err)
		got = append(got, rec)
	}
	is.True(got != nil)
	is.Equal(5, got.Len())
	// sort the records then assert the values.
	sort.Sort(got)
	for i, rec := range got {
		is.Equal(rec.Payload.After, opencdc.StructuredData{PartitionKey: fmt.Sprintf("pkey%d", i), SortKey: fmt.Sprintf("%d", i)})
	}
	_ = source.Teardown(ctx)
}

func TestSource_SnapshotRestart(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := prepareIntegrationTest(ctx, t)
	testTable := cfg[SourceConfigTable]
	source := &Source{}
	err := source.Configure(ctx, cfg)
	is.NoErr(err)

	// add rows
	err = insertRecord(ctx, client, testTable, 0, 6)
	is.NoErr(err)

	// set a non nil position
	pos := position.Position{
		IteratorType: position.TypeSnapshot,
		PartitionKey: "pkey2",
		SortKey:      "2",
	}
	recPos, err := pos.ToRecordPosition()
	is.NoErr(err)
	err = source.Open(ctx, recPos)
	is.NoErr(err)

	var got Records
	for i := 0; i < 6; i++ {
		rec, err := source.Read(ctx)
		is.NoErr(err)
		got = append(got, rec)
	}
	// if the read records are five, then the snapshot started again successfully, from nil position
	is.True(got != nil)
	is.Equal(6, got.Len())
}

func TestSource_EmptyTable(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := prepareIntegrationTest(ctx, t)
	testTable := cfg[SourceConfigTable]

	source := &Source{}
	err := source.Configure(ctx, cfg)
	is.NoErr(err)
	err = source.Open(ctx, nil)
	is.NoErr(err)

	_, err = source.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))
	time.Sleep(1 * time.Second)

	// test CDC after an empty snapshot.
	err = insertRecord(ctx, client, testTable, 0, 1)
	is.NoErr(err)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		rec, err := source.Read(timeoutCtx)
		if errors.Is(err, sdk.ErrBackoffRetry) {
			continue
		}
		is.NoErr(err)
		is.Equal(rec.Payload.After, opencdc.StructuredData{PartitionKey: "pkey0", SortKey: "0"})
		is.Equal(rec.Operation, opencdc.OperationCreate)
		break
	}

	_ = source.Teardown(ctx)
}

func TestSource_NonExistentTable(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	_, cfg := prepareIntegrationTest(ctx, t)

	source := &Source{}

	// set the table name to a unique uuid, so it doesn't exist.
	cfg[SourceConfigTable] = uuid.NewString()

	err := source.Configure(ctx, cfg)
	is.NoErr(err)

	// table existence check at "Open"
	err = source.Open(ctx, nil)
	is.True(err != nil)
}

func TestSource_CDC(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client, cfg := prepareIntegrationTest(ctx, t)
	//
	testTable := cfg[SourceConfigTable]
	source := &Source{}
	err := source.Configure(ctx, cfg)
	is.NoErr(err)

	// add rows
	err = insertRecord(ctx, client, testTable, 1, 2)
	is.NoErr(err)

	err = source.Open(ctx, nil)
	is.NoErr(err)

	// snapshot, one record
	rec, err := source.Read(ctx)
	is.NoErr(err)
	is.Equal(rec.Payload.After, opencdc.StructuredData{PartitionKey: "pkey1", SortKey: "1"})

	time.Sleep(1 * time.Second)
	// add a row, will be captured by CDC
	err = insertRecord(ctx, client, testTable, 2, 3)
	is.NoErr(err)

	// update the latest row, will be captured by CDC
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(testTable),
		Key: map[string]types.AttributeValue{
			PartitionKey: &types.AttributeValueMemberS{Value: "pkey1"}, // partition key
			SortKey:      &types.AttributeValueMemberN{Value: "1"},     // sort key
		},
		UpdateExpression: aws.String("SET #attr = :newValue"),
		ExpressionAttributeNames: map[string]string{
			"#attr": "data", // alias 'data' to avoid reserved keyword error
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newValue": &types.AttributeValueMemberS{Value: "newValue"}, // the new value
		},
	})
	is.NoErr(err)

	// delete the latest row, will be captured by CDC
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(testTable),
		Key: map[string]types.AttributeValue{
			PartitionKey: &types.AttributeValueMemberS{Value: "pkey2"},
			SortKey:      &types.AttributeValueMemberN{Value: "2"},
		},
	})
	is.NoErr(err)

	// cdc
	i := 0
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		rec2, err := source.Read(timeoutCtx)
		if err == nil {
			switch i {
			case 0:
				is.True(rec2.Operation == opencdc.OperationCreate)
				is.Equal(rec2.Payload.After, opencdc.StructuredData{PartitionKey: "pkey2", SortKey: "2"})
			case 1:
				is.True(rec2.Operation == opencdc.OperationUpdate)
				is.Equal(rec2.Payload.After, opencdc.StructuredData{"data": "newValue", PartitionKey: "pkey1", SortKey: "1"})
			case 2:
				is.True(rec2.Operation == opencdc.OperationDelete)
				is.Equal(rec2.Payload.Before, opencdc.StructuredData{PartitionKey: "pkey2", SortKey: "2"})
			}
			i++
		}
		if i == 3 {
			break
		}
	}

	_ = source.Teardown(ctx)
}

func prepareIntegrationTest(ctx context.Context, t *testing.T) (*dynamodb.Client, map[string]string) {
	t.Helper()

	// default params, connects to DynamoDB docker instance.
	cfg := map[string]string{
		SourceConfigAwsAccessKeyId:         "test",
		SourceConfigAwsSecretAccessKey:     "test",
		SourceConfigAwsRegion:              "us-east-1",
		SourceConfigDiscoveryPollingPeriod: "5s",
		SourceConfigRecordsPollingPeriod:   "1s",
		SourceConfigAwsUrl:                 "http://localhost:4566", // docker url
	}

	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion != "" && awsAccessKeyID != "" && awsSecretAccessKey != "" {
		cfg = map[string]string{
			SourceConfigAwsAccessKeyId:         awsAccessKeyID,
			SourceConfigAwsSecretAccessKey:     awsSecretAccessKey,
			SourceConfigAwsRegion:              awsRegion,
			SourceConfigDiscoveryPollingPeriod: "5s",
			SourceConfigRecordsPollingPeriod:   "1s",
			SourceConfigAwsUrl:                 "", // empty, so real AWS DynamoDB will be used instead.
		}
	}

	client, err := newDynamoClients(ctx, cfg)
	if err != nil {
		t.Fatalf("could not create dynamoDB clients: %v", err)
	}

	table := "conduit-dynamodb-source-test-" + uuid.NewString()
	cfg[SourceConfigTable] = table

	// create table
	err = createTable(ctx, client, table, PartitionKey, SortKey)
	if err != nil {
		t.Fatalf("could not create dynamoDB table: %v", err)
	}

	t.Cleanup(func() {
		err := deleteTable(ctx, client, table)
		if err != nil {
			t.Logf("failed to delete the table: %v", err)
		}
	})

	return client, cfg
}

func newDynamoClients(ctx context.Context, cfg map[string]string) (*dynamodb.Client, error) {
	clientCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg[SourceConfigAwsRegion]),
		config.WithCredentialsProvider(aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg[SourceConfigAwsAccessKeyId], cfg[SourceConfigAwsSecretAccessKey], ""))),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating AWS session: %w", err)
	}

	var dynamoDBClient *dynamodb.Client
	if cfg[SourceConfigAwsUrl] != "" {
		dynamoDBClient = dynamodb.NewFromConfig(clientCfg, func(o *dynamodb.Options) {
			o.EndpointResolverV2 = staticResolver{
				BaseURL: cfg[SourceConfigAwsUrl],
			}
		})
	} else {
		dynamoDBClient = dynamodb.NewFromConfig(clientCfg)
	}
	return dynamoDBClient, nil
}

func insertRecord(ctx context.Context, client *dynamodb.Client, tableName string, from int, to int) error {
	for i := 0; i < to-from; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				PartitionKey: &types.AttributeValueMemberS{Value: fmt.Sprintf("pkey%d", from+i)},
				SortKey:      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", from+i)},
			},
		})
		if err != nil {
			return fmt.Errorf("error inserting record: %w", err)
		}
	}
	return nil
}

func createTable(ctx context.Context, client *dynamodb.Client, tableName string, partitionKey string, sortKey string) error {
	// Define the table schema with additional attributes
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(partitionKey),   // Partition key
				AttributeType: types.ScalarAttributeTypeS, // String
			},
			{
				AttributeName: aws.String(sortKey),        // Sort key
				AttributeType: types.ScalarAttributeTypeN, // number
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(partitionKey), // Partition key
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(sortKey), // Sort key
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	}

	// Create the table
	_, err := client.CreateTable(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Loop to wait for the table to become active
	for {
		output, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return fmt.Errorf("failed to describe table: %w", err)
		}

		// Check the table status
		if output.Table.TableStatus == types.TableStatusActive {
			fmt.Printf("Table %s is now active.\n", tableName)
			break
		}

		fmt.Printf("Table %s status: %s. Waiting...\n", tableName, output.Table.TableStatus)
		time.Sleep(5 * time.Second) // Wait before checking again
	}

	return nil
}

func deleteTable(ctx context.Context, client *dynamodb.Client, tableName string) error {
	// Create the input for the DeleteTable call
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}

	// Delete the table
	_, err := client.DeleteTable(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	return nil
}

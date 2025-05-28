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
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestDestination_WriteAndDelete(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	client, cfg := prepareDestinationIntegrationTest(ctx, t)
	dest := &Destination{}
	defer func() {
		err := dest.Teardown(ctx)
		is.NoErr(err)
	}()
	err := sdk.Util.ParseConfig(ctx, cfg, dest.Config(), Connector.NewSpecification().SourceParams)
	is.NoErr(err)

	err = dest.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}

	records := []opencdc.Record{
		{
			Position:  nil,
			Operation: opencdc.OperationCreate,
			Key: opencdc.StructuredData{
				"pkey": "testkey",
				"skey": 1,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"pkey": "testkey",
					"skey": 1,
					"val":  "hello1",
				},
			},
		},
		{
			Position:  nil,
			Operation: opencdc.OperationCreate,
			Key: opencdc.StructuredData{
				"pkey": "testkey",
				"skey": 2,
			},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"pkey": "testkey",
					"skey": 2,
					"val":  "hello2",
				},
			},
		},
	}

	_, err = dest.Write(ctx, records)
	is.NoErr(err)

	// Check that record exists
	input := &dynamodb.GetItemInput{
		TableName: aws.String(cfg["table"]),
		Key: map[string]types.AttributeValue{
			"pkey": &types.AttributeValueMemberS{Value: "testkey"},
			"skey": &types.AttributeValueMemberN{Value: "1"},
		},
	}
	// first record
	out, err := client.GetItem(ctx, input)
	is.NoErr(err)
	is.True(out.Item != nil)
	is.Equal(out.Item["val"].(*types.AttributeValueMemberS).Value, "hello1")
	// second record
	input.Key["skey"] = &types.AttributeValueMemberN{Value: "2"}
	out2, err := client.GetItem(ctx, input)
	is.NoErr(err)
	is.True(out2.Item != nil)
	is.Equal(out2.Item["val"].(*types.AttributeValueMemberS).Value, "hello2")

	// Now test delete
	records[1].Operation = opencdc.OperationDelete
	_, err = dest.Write(ctx, []opencdc.Record{records[1]}) // delete the second record
	is.NoErr(err)

	out, err = client.GetItem(ctx, input) // make sure second record is deleted
	is.NoErr(err)
	is.True(len(out.Item) == 0) // deleted
}

func prepareDestinationIntegrationTest(ctx context.Context, t *testing.T) (*dynamodb.Client, map[string]string) {
	t.Helper()
	// default params, connects to DynamoDB docker instance.
	cfg := map[string]string{
		ConfigAwsAccessKeyID:     "test",
		ConfigAwsSecretAccessKey: "test",
		ConfigAwsRegion:          "us-east-1",
		ConfigAwsURL:             "http://localhost:4566", // docker url
	}

	awsAccessKeyID := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecretAccessKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion != "" && awsAccessKeyID != "" && awsSecretAccessKey != "" {
		cfg = map[string]string{
			ConfigAwsAccessKeyID:     awsAccessKeyID,
			ConfigAwsSecretAccessKey: awsSecretAccessKey,
			ConfigAwsRegion:          awsRegion,
			ConfigAwsURL:             "", // empty, so real AWS DynamoDB will be used instead.
		}
	}

	client, err := newDynamoClients(ctx, cfg)
	if err != nil {
		t.Fatalf("could not create dynamoDB clients: %v", err)
	}

	table := "conduit-dynamodb-destination-test-" + uuid.NewString()
	cfg[ConfigTable] = table

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

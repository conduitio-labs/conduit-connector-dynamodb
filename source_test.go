package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	stypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	cconfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := NewSource()
	cfg := cconfig.Config{
		"table":                              "Employees",
		"key":                                "key",
		"aws.region":                         "us-east-1",
		"aws.accessKeyId":                    "todo",
		"aws.secretAccessKey":                "todo",
		"sdk.schema.extract.key.enabled":     "false",
		"sdk.schema.extract.payload.enabled": "false",
	}
	err := con.Configure(ctx, cfg)
	is.NoErr(err)
	err = con.Open(ctx, opencdc.Position{})
	is.NoErr(err)
	rec, err := con.Read(ctx)
	is.NoErr(err)
	fmt.Println(rec)
	rec, err = con.Read(ctx)
	is.NoErr(err)
	fmt.Println(rec)
	rec, err = con.Read(ctx)
	is.True(errors.Is(err, sdk.ErrBackoffRetry))
	is.Equal(rec, opencdc.Record{})
	err = con.Teardown(context.Background())
	is.NoErr(err)
}

func TestTeardownSource_Streams(t *testing.T) {
	//is := is.New(t)
	// Set up the AWS session and DynamoDB service client
	sess, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}

	client := dynamodb.NewFromConfig(sess)

	// Specify the table name you want to describe
	tableName := "Employees"

	// Describe the table
	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	result, err := client.DescribeTable(context.Background(), describeTableInput)
	if err != nil {
		log.Fatalf("Failed to describe DynamoDB table: %v", err)
	}
	fmt.Println("ARN: ", *result.Table.LatestStreamArn)

	//err = scanTable(client, tableName)
	//if err != nil {
	//	fmt.Println(err)
	//}

	ctx := context.Background()
	// Create DynamoDB Streams client
	streamsClient := dynamodbstreams.NewFromConfig(sess)
	// Get the shard iterator for the stream
	shardIterator, err := getShardIterator(ctx, streamsClient, *result.Table.LatestStreamArn, stypes.ShardIteratorTypeLatest)
	if err != nil {
		log.Fatalf("Failed to get shard iterator: %v", err)
	}

	// continuously poll for new records
	for {
		fmt.Println("looping")
		out, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIterator,
			//Limit:         aws.Int32(1000),
		})
		if err != nil {
			log.Fatalf("Failed to get records: %v", err)
		}

		// process the stream records
		for _, record := range out.Records {
			fmt.Printf("New record: %+v\n", record)
		}

		// update the shard iterator
		shardIterator = out.NextShardIterator
		fmt.Println(shardIterator)

		if shardIterator == nil {
			shardIterator, err = getShardIterator(ctx, streamsClient, *result.Table.LatestStreamArn, stypes.ShardIteratorTypeLatest)
			if err != nil {
				log.Fatalf("Failed to get shard iterator: %v", err)
			}
		}

		// sleep before polling again
		time.Sleep(time.Second)
		//time.Sleep(20 * time.Millisecond)
	}

}

// getShardIterator gets the initial shard iterator for the stream
func getShardIterator(ctx context.Context, client *dynamodbstreams.Client, streamArn string, typ stypes.ShardIteratorType) (*string, error) {
	// Describe the stream to get the shard ID
	describeStreamOutput, err := client.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamArn),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe stream: %w", err)
	}
	//fmt.Printf("describe stream: %#v", describeStreamOutput.StreamDescription)

	// Assume there is at least one shard
	shardID := describeStreamOutput.StreamDescription.Shards[0].ShardId

	// Get the shard iterator
	getShardIteratorOutput, err := client.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamArn),
		ShardId:           shardID,
		ShardIteratorType: typ,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get shard iterator: %w", err)
	}

	return getShardIteratorOutput.ShardIterator, nil
}

func scanTable(dynamoSvc *dynamodb.Client, tableName string) error {
	var lastEvaluatedKey map[string]types.AttributeValue

	// Scan the table until all items are retrieved
	for {
		// Create the scan input, optionally with a limit for the number of items per page
		scanInput := &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ExclusiveStartKey: lastEvaluatedKey, // Start scanning from the last evaluated key
		}

		// Perform the scan operation
		scanOutput, err := dynamoSvc.Scan(context.Background(), scanInput)
		if err != nil {
			return fmt.Errorf("failed to scan DynamoDB table: %v", err)
		}

		// Process the scanned
		fmt.Println("item size: ", len(scanOutput.Items))
		fmt.Println("ITEMS:")
		for _, item := range scanOutput.Items {
			str := fmt.Sprintf("%v", item)
			if len(str) > 30 {
				str = str[:30]
			}
			// Create the record
			rec := sdk.Util.Source.NewRecordSnapshot(
				opencdc.Position{},
				map[string]string{
					"Table": tableName,
				},
				opencdc.StructuredData{
					"key": item["key"],
				},
				opencdc.StructuredData{
					"payload": str,
				},
			)
			fmt.Printf("record: %v\n", rec)
		}

		fmt.Println("Last Evaluated Key:", scanOutput.LastEvaluatedKey)
		// Check if there is more data to be scanned
		if scanOutput.LastEvaluatedKey == nil {
			break
		}
		// Update the last evaluated key to continue scanning
		lastEvaluatedKey = scanOutput.LastEvaluatedKey
	}

	return nil
}

//func TestTeardownSource_LargeItem(t *testing.T) {
//	// Load the AWS configuration
//	sess, err := session.NewSession(&aws.Config{
//		Region: aws.String("us-east-1"),
//	})
//	if err != nil {
//		log.Fatalf("Failed to create AWS session: %v", err)
//	}
//
//	dynamoSvc := dynamodb.New(sess)
//
//	// Specify the table name you want to describe
//	tableName := "Employees"
//
//	// Create a very large string close to 400kb in size
//	largeString := strings.Repeat("D", 300*1024) // 1 MB minus 100 bytes for the attribute name and overhead
//	type Item struct {
//		Key  string `json:"key"`
//		Name string
//	}
//	item := Item{
//		Key:  "key7",
//		Name: largeString,
//	}
//
//	av, err := dynamodbattribute.MarshalMap(item)
//	if err != nil {
//		log.Fatalf("Got error marshalling new movie item: %s", err)
//	}
//
//	// Put the item into the DynamoDB table
//	_, err = dynamoSvc.PutItem(&dynamodb.PutItemInput{
//		Table: aws.String(tableName),
//		Item:      av,
//	})
//
//	if err != nil {
//		fmt.Println("Error putting item:", err)
//		return
//	}
//
//	fmt.Println("Item successfully put into DynamoDB")
//}

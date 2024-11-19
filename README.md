# Conduit Connector for DynamoDB
The DynamoDB connector is one of [Conduit](https://github.com/ConduitIO/conduit) standalone plugins. It provides a source
connector for [DynamoDB](https://aws.amazon.com/dynamodb/).

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test-integration` to run all the integration tests. Tests require Docker to be installed and running.
The command will handle starting and stopping docker containers for you.

If you want to run the integration tests against your AWS DynamoDB instead of docker, You must set the environment
variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) before running `make test`, 
or before running the tests manually.

## Source
A source connector that pulls data from a DynamoDB table to downstream resources via Conduit.

The connector starts with a snapshot of the data currently existent in the table, sends these records to the 
destination, then starts the CDC (Change Data Capture) mode which will listen to events happening on the table
in real-time, and sends these events' records to the destination (these events include: `updates`, `deletes`, and `inserts`).
You can opt out from taking the snapshot by setting the parameter `skipSnapshot` to `true`, meaning that only the CDC
events will be captured.

The source connector uses [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) to get CDC events,
so you need to enable the stream before running the connector. Check out the documentation for [how to enable a DynamoDB Stream](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Enabling).

### Configuration

| name                     | description                                                                                                                       | required | default | example               |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------|----------|---------|-----------------------|
| `table`                  | Table is the DynamoDB table name to pull data from.                                                                               | true     |         | Employees             |
| `aws.region`             | AWS region.                                                                                                                       | true     |         | us-east-1             |
| `aws.accessKeyId`        | AWS access key id.                                                                                                                | true     |         | MY_ACCESS_KEY_ID      |
| `aws.secretAccessKey`    | AWS secret access key.                                                                                                            | true     |         | MY_SECRET_ACCESS_KEY  |
| `aws.url`                | The URL for AWS (useful when testing the connector with localstack).                                                              | false    |         | http://localhost:4566 |
| `discoveryPollingPeriod` | Polling period for the CDC mode of how often to check for new shards in the DynamoDB Stream, formatted as a time.Duration string. | false    | 10s     | 100ms, 1m, 10m, 1h    |
| `recordsPollingPeriod`   | Polling period for the CDC mode of how often to get new records from a shard, formatted as a time.Duration string.                | false    | 5s      | 100ms, 1m, 10m, 1h    |
| `skipSnapshot`           | Determines weather to skip the snapshot or not.                                                                                   | false    | false   | true                  |

<!-- Todo: working on adding some implementation details -->

![scarf pixel connector-dynamodb-readme](https://static.scarf.sh/a.png?x-pxid=cbb3901b-e502-4106-aa10-0b0726532dd6)
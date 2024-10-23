# Conduit Connector for DynamoDB
![connector-dynamodb-readme](https://static.scarf.sh/a.png?x-pxid=cbb3901b-e502-4106-aa10-0b0726532dd6)
The DynamoDB connector is one of [Conduit](https://github.com/ConduitIO/conduit) standalone plugins. It provides a source
connector for [DynamoDB](https://aws.amazon.com/dynamodb/).

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests.

Run `make test-integration` to run all the integration tests. Tests require Docker to be installed and running.
The command will handle starting and stopping docker containers for you.

## Source
A source connector that pulls data from a DynamoDB table to downstream resources via Conduit.

The connector starts with a snapshot of the data currently existent in the table, sends these records to the 
destination, then starts the CDC (Change Data Capture) mode which will listen to events happening on the table
in real-time, and sends these event records to the destination (these events include: `updates`, `deletes`, and `inserts`).

For the source connector to get CDC events, it uses [DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html), 
so you need to enable the stream before running the connector, check these docs for [how to enable your DynamoDB Stream](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html#Streams.Enabling).

### Configuration

| name                  | description                                                           | required | default | example               |
|-----------------------|-----------------------------------------------------------------------|----------|---------|-----------------------|
| `table`               | Table is the DynamoDB table name to pull data from.                   | true     |         | Employees             |
| `aws.region`          | AWS region.                                                           | true     |         | us-east-1             |
| `aws.accessKeyId`     | AWS access key id.                                                    | true     |         | MY_ACCESS_KEY_ID      |
| `aws.secretAccessKey` | AWS secret access key.                                                | true     |         | MY_SECRET_ACCESS_KEY  |
| `aws.url`             | The URL for AWS (useful when testing the connector with localstack).  | false    |         | http://localhost:4566 |
| `pollingPeriod`       | Polling period for the CDC mode, formatted as a time.Duration string. | false    | 1s      | 100ms, 1m, 10m, 1h    |
| `skipSnapshot`        | Determines weather to skip the snapshot or not.                       | false    | false   | true                  |

<!-- Todo: working on adding some implementation details -->

![scarf pixel conduit-site-docs-features](https://static.scarf.sh/a.png?x-pxid=cbb3901b-e502-4106-aa10-0b0726532dd6)
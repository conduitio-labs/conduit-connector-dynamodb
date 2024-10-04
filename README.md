# Conduit Connector for DynamoDB

[Conduit](https://conduit.io) connector for [DynamoDB](https://aws.amazon.com/dynamodb/).

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. 

## Source

A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

### Configuration

| name                  | description                                                           | required | default | example              |
|-----------------------|-----------------------------------------------------------------------|----------|---------|----------------------|
| `table`               | Table is the DynamoDB table name to pull data from.                   | true     |         | Employees            |
| `aws.region`          | AWS region.                                                           | true     |         | us-east-1            |
| `aws.accessKeyId`     | AWS access key id.                                                    | true     |         | MY_ACCESS_KEY_ID     |
| `aws.secretAccessKey` | AWS secret access key.                                                | true     |         | MY_SECRET_ACCESS_KEY |
| `pollingPeriod`       | Polling period for the CDC mode, formatted as a time.Duration string. | false    | 1s      | 100ms, 1m, 10m, 1h   |
| `skipSnapshot`        | Determines weather to skip the snapshot or not.                       | false    | false   | true                 |


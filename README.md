# Conduit Connector for <!-- readmegen:name -->Dynamodb<!-- /readmegen:name -->

[Conduit](https://conduit.io) connector for <!-- readmegen:name -->Dynamodb<!-- /readmegen:name -->.

<!-- readmegen:description -->
A DynamoDB source plugin for Conduit, it scans the table at the beginning taking a snapshot, then starts listening to CDC events using DynamoDB streams.<!-- /readmegen:description -->

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

<!-- readmegen:source.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "dynamodb"
        settings:
          # AWS access key id.
          # Type: string
          # Required: yes
          aws.accessKeyId: ""
          # AWS region.
          # Type: string
          # Required: yes
          aws.region: ""
          # AWS secret access key.
          # Type: string
          # Required: yes
          aws.secretAccessKey: ""
          # Table is the DynamoDB table name to pull data from.
          # Type: string
          # Required: yes
          table: ""
          # AWS temporary session token. Note that to keep the connector running
          # long-term, you should use an IAM user with no temporary session
          # token. If the session token is used, then the connector will fail
          # once it expires.
          # Type: string
          # Required: no
          aws.sessionToken: ""
          # AWSURL The URL for AWS (useful when testing the connector with
          # localstack).
          # Type: string
          # Required: no
          aws.url: ""
          # Discovery polling period for the CDC mode of how often to check for
          # new shards in the DynamoDB Stream, formatted as a time.Duration
          # string.
          # Type: duration
          # Required: no
          discoveryPollingPeriod: "10s"
          # Records polling period for the CDC mode of how often to get new
          # records from a shard, formatted as a time.Duration string.
          # Type: duration
          # Required: no
          recordsPollingPeriod: "1s"
          # SkipSnapshot determines weather to skip the snapshot or not.
          # Type: bool
          # Required: no
          skipSnapshot: "false"
          # Maximum delay before an incomplete batch is read from the source.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets read from the source.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Specifies whether to use a schema context name. If set to false, no
          # schema context name will be used, and schemas will be saved with the
          # subject name specified in the connector (not safe because of name
          # conflicts).
          # Type: bool
          # Required: no
          sdk.schema.context.enabled: "true"
          # Schema context name to be used. Used as a prefix for all schema
          # subject names. If empty, defaults to the connector ID.
          # Type: string
          # Required: no
          sdk.schema.context.name: ""
          # Whether to extract and encode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # The subject of the key schema. If the record metadata contains the
          # field "opencdc.collection" it is prepended to the subject name and
          # separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.key.subject: "key"
          # Whether to extract and encode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
          # The subject of the payload schema. If the record metadata contains
          # the field "opencdc.collection" it is prepended to the subject name
          # and separated with a dot.
          # Type: string
          # Required: no
          sdk.schema.extract.payload.subject: "payload"
          # The type of the payload schema.
          # Type: string
          # Required: no
          sdk.schema.extract.type: "avro"
```
<!-- /readmegen:source.parameters.yaml -->

![scarf pixel connector-dynamodb-readme](https://static.scarf.sh/a.png?x-pxid=cbb3901b-e502-4106-aa10-0b0726532dd6)
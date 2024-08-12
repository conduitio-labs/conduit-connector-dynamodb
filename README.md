# Conduit Connector for <resource>

[Conduit](https://conduit.io) connector for DynamoDB.

## How to build?

Run `make build` to build the connector.

## Testing

Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Source

A source connector pulls data from an external resource and pushes it to downstream resources via Conduit.

### Configuration

| name                  | description                           | required | default value |
|-----------------------|---------------------------------------|----------|---------------|
| `source_config_param` | Description of `source_config_param`. | true     | 1000          |


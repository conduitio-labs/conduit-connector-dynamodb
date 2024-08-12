package main

import (
	dynamodb "github.com/conduitio-labs/conduit-connector-dynamodb"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(dynamodb.Connector)
}

package dynamodb

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "dynamodb",
		Summary: "A DynamoDB source plugin for Conduit",
		Description: "A DynamoDB source plugin for Conduit, it scans the table at the beginning taking a snapshot, " +
			"then starts listening to CDC events using DynamoDB streams.",
		Version: version,
		Author:  "Meroxa, Inc.",
	}
}

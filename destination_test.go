package dynamodb_test

import (
	"context"
	"testing"

	dynamodb "github.com/conduitio-labs/conduit-connector-dynamodb"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := dynamodb.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

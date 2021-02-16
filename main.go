package dgraphdal

import (
	"context"
	"fmt"

	// version of dgraph to import
	// old versions require a diffrent client of dgraph
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// !+NewClient establish a new connection
// TODO: implement security
//
func NewClient(ip string) (*dgo.Dgraph, error) {
	d, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("error grpc dial %s: %v", ip, err)
	}
	return dgo.NewDgraphClient(api.NewDgraphClient(d)), nil
}

// !-NewClient

// !+Query
// run a query on dgraph
// return result and error
func Query(dg *dgo.Dgraph, s string) ([]byte, error) {
	resp, err := dg.NewTxn().Query(context.Background(), s)
	if err != nil {
		return nil, fmt.Errorf("query : %v", err)
	}
	return resp.GetJson(), nil
}

// !-Query

// !+Mutate
// Mutate (Update or Create) an edge
// return -> response ( operation details), error what may have gone wrong
func Mutate(dg *dgo.Dgraph, pb []byte) (*api.Response, error) {
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   pb,
	}

	resp, err := dg.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		return nil, fmt.Errorf("migrating new schema: %v", err)
	}
	return resp, nil
}

// !-Mutate

// !+Delete
// delete an edge
// return -> response(operation details) , discriptive error what may have gone wrong
func Delete(dg *dgo.Dgraph, pb []byte) (*api.Response, error) {
	mu := &api.Mutation{
		CommitNow:  true,
		DeleteJson: pb,
	}

	resp, err := dg.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		return nil, fmt.Errorf("migrating new schema: %v", err)
	}
	return resp, nil
}

// !-Delete

// !+Migrate
// alter the schema on the database
// must insert auth token when u set it on dgraph otherwise give it an empty string
func Migrate(dg *dgo.Dgraph, schema, token string) error {
	md := metadata.New(nil)
	md.Append("auth-token", token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	err := dg.Alter(ctx, &api.Operation{Schema: schema})
	if err != nil {
		return fmt.Errorf("migrating schema: %v", err)
	}
	return nil
}

// !-Migrate

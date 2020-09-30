package dgraphdal

import (
	"context"
	"fmt"
	"log"
	"time"

	// version of dgraph to import
	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

//!+NewClient establish a new connection
//TODO: implement security
func NewClient(ip string) *dgo.Dgraph {
	// Dial a gRPC connection. The address to dial to can be configured when
	// setting up the dgraph cluster.
	d, err := grpc.Dial(ip, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("grpc dial %s: %v", ip, err)
	}

	return dgo.NewDgraphClient(
		api.NewDgraphClient(d),
	)
}

//!-NewClient

//!+Query
func Query(dg *dgo.Dgraph, s string) ([]byte, error) {
	start := time.Now()

	resp, err := dg.NewTxn().Query(context.Background(), s)
	if err != nil {
		return nil, fmt.Errorf("query : %v", err)
	}

	log.Println(time.Since(start))
	return resp.GetJson(), nil
}

//!-Query

//!+Mutate send a mutation to dgraph
func Mutate(dg *dgo.Dgraph, pb []byte) error {
	start := time.Now()
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   pb,
	}

	_, err := dg.NewTxn().Mutate(context.Background(), mu)
	if err != nil {
		return fmt.Errorf("migrating new schema: %v", err)
	}

	log.Println(time.Since(start))
	return nil
}

//!-Mutate

//!+Migrate alter the schema on the database
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

//!-Migrate

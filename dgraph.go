package dgraphdal

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"google.golang.org/grpc"
)

//+ NewClient
// establish connection and return
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

//!+ Query
// connect and query
// param s: the query u want to run
// return: resp as []byte
func Query(dg *dgo.Dgraph, s string) ([]byte, error) {
	start := time.Now()

	resp, err := dg.NewTxn().Query(context.Background(), s)
	if err != nil {
		return nil, fmt.Errorf("query : %v", err)
	}

	log.Println(time.Since(start))
	return resp.GetJson(), nil
}

//!- Query

//!+ Mutate
// Mutate sends a mutation request to the dgraph server
func Mutate(dg *dgo.Dgraph, pb []byte) error {
	start := time.Now()
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   pb,
	}
	// TODO: check resp
	resp, err := dg.NewTxn().Mutate(context.Background(), mu)
	fmt.Println(resp.Json)
	if err != nil {
		return fmt.Errorf("migrating new schema: %v", err)
	}

	log.Println(time.Since(start))
	return nil
}

//!- Mutate

//!+ Migrate
// Migrate alter the database schema
func Migrate(dg *dgo.Dgraph, schema string) error {
	op := &api.Operation{}
	op.Schema = schema
	ctx := context.Background()
	err := dg.Alter(ctx, op)
	if err != nil {
		return fmt.Errorf("migrating schema: %v", err)
	}
	return nil
}

//!- Migrate

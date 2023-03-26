package blobstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// This test requires a DB connection to a mongodb instance. The
// connection string can be set in an environment variable as:
//
//   mongo_uri="mongodb://host:port
//
// If not given, mongodb://127.0.0.1:27017 will be assumed.
//
// It creates a test database and blob collection, creates some
// documents, and drops the collection.

func cleanupBlobs(s *Store) {
	s.Collection.Drop(context.Background())
}

func setupTestConnection() *mongo.Client {
	adr := os.Getenv("mongo_uri")
	if len(adr) == 0 {
		adr = "mongodb://127.0.0.1:27017"
	}
	fmt.Printf("Connecting to %s \n", adr)
	opts := options.Client()
	opts.ApplyURI(adr)
	cli, err := mongo.NewClient(opts)
	if err != nil {
		panic(fmt.Sprintf("Cannot connect %s", adr))
	}
	err = cli.Connect(context.Background())
	if err != nil {
		panic(err)
	}
	return cli
}

func TestBlob(t *testing.T) {
	cli := setupTestConnection()
	store := &Store{
		Collection: cli.Database("test").Collection("blob"),
		ChunkSize:  1024,
	}
	if err := store.EnsureIndex(context.Background()); err != nil {
		t.Error(err)
	}
	defer cleanupBlobs(store)
	rdata := func(n int) []byte {
		data := make([]byte, n)
		for i := range data {
			data[i] = byte(rand.Int())
		}
		return data
	}
	size := 5005
	data := rdata(size)
	err := store.Write(context.Background(), "1", bytes.NewReader(data))
	if err != nil {
		t.Error(err)
	}

	rd, err := store.Read(context.Background(), "1")
	if err != nil {
		t.Error(err)
	}
	read, err := io.ReadAll(rd)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(read, data) {
		t.Errorf("Not equal")
	}

	n, err := store.Size(context.Background(), "1")
	if err != nil {
		t.Error(err)
	}
	if n != int64(size) {
		t.Errorf("Wrong size: %d", n)
	}

	store.Remove(context.Background(), "1")

	_, err = store.Read(context.Background(), "1")

	if err == nil {
		t.Errorf("Error expected")
	}
}

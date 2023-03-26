package blobstore

import (
	"context"
	"errors"
	"io"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type blobSegment struct {
	ID    string `bson:"blobId"`
	Seq   uint64 `bson:"seq"`
	Data  []byte `bson:"data"`
	Start uint64 `bson:"s"`
	N     uint64 `bson:"n"`
}

var DefaultChunkSize = 2048 * 1024 // 2MB chunks

var ErrNotFound = errors.New("Not found")

type Store struct {
	Collection *mongo.Collection
	ChunkSize  int

	index sync.Once
}

// EnsureIndex ensures that the collection has an index on id and
// seq. This can be called multiple times on a store object.
func (store *Store) EnsureIndex(ctx context.Context) (err error) {
	store.index.Do(func() {
		ix := store.Collection.Indexes()
		_, err = ix.CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{
				{Key: "blobId", Value: 1},
				{Key: "seq", Value: 1},
			},
			Options: options.Index().SetUnique(true),
		})
	})
	return
}

// Remove all given blobs
func (store *Store) Remove(ctx context.Context, blobIDs ...string) error {
	if len(blobIDs) == 0 {
		return nil
	}
	_, err := store.Collection.DeleteMany(ctx, bson.M{"blobId": bson.M{"$in": blobIDs}})
	return err
}

// Write blob data. Data can be nil, if so, a truncated blob will be written
func (store *Store) Write(ctx context.Context, blobID string, data io.Reader) error {
	var segment blobSegment
	chunkSize := store.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	segment.Data = make([]byte, chunkSize)
	segment.ID = blobID
	start := uint64(0)
	for {
		n, err := io.ReadAtLeast(data, segment.Data, len(segment.Data))
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// n > 0 is guaranteed
			segment.Data = segment.Data[:n]
			err = nil
		} else if errors.Is(err, io.EOF) {
			// Nothing read
			break
		} else if err != nil {
			return err
		}
		segment.Start = start
		segment.N = uint64(len(segment.Data))
		start += uint64(len(segment.Data))
		_, err = store.Collection.ReplaceOne(ctx, bson.M{"blobId": segment.ID, "seq": segment.Seq}, segment, options.Replace().SetUpsert(true))
		if err != nil {
			return err
		}
		segment.Seq++
	}
	// Remove remaining segments
	_, err := store.Collection.DeleteMany(ctx, bson.M{"blobId": segment.ID, "seq": bson.M{"$gte": segment.Seq}})
	return err
}

// Read blob data. To stop reading, close the returned readCloser. You
// must close the returned stream, otherwise the goroutine streaming
// the data will leak.
func (store *Store) Read(ctx context.Context, blobID string) (io.ReadCloser, error) {
	rd, wr := io.Pipe()
	cursor, err := store.Collection.Find(ctx, bson.M{"blobId": blobID}, options.Find().SetSort(map[string]interface{}{"seq": 1}))
	if err != nil {
		return nil, err
	}
	if !cursor.Next(ctx) {
		cursor.Close(ctx)
		return nil, ErrNotFound
	}
	go func() {
		defer wr.Close()
		defer cursor.Close(context.Background())
		var segment blobSegment
		cursor.Decode(&segment)
		if _, err := wr.Write(segment.Data); err != nil {
			return
		}
		for cursor.Next(ctx) {
			cursor.Decode(&segment)
			if _, err := wr.Write(segment.Data); err != nil {
				return
			}
		}
	}()
	return rd, nil
}

// Size returns the size of the object
func (store *Store) Size(ctx context.Context, blobID string) (int64, error) {
	cursor, err := store.Collection.Find(ctx, bson.M{"blobId": blobID}, options.Find().SetSort(map[string]interface{}{"seq": -1}))
	if err != nil {
		return 0, err
	}
	if !cursor.Next(ctx) {
		cursor.Close(ctx)
		return 0, ErrNotFound
	}
	var last blobSegment
	cursor.Decode(&last)
	return int64(last.Start + last.N), nil
}

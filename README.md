# Blobstore for MongoDB

This is intended to be a simple replacement for binary object storage
in MongoDB. GridFS does not play well with transactions, and it looks
too much like a file system so using it for generic BLOB storage is
usually inconvenient. This Go library provides a simple replacement
for it.

Create a blob store using:

``` go
store := &blobstore.Store {
   Collection: coll,
}
```

Make sure indexes are created for the store. This will run once for
the lifetime of the store.

``` go
store.EnsureIndex(context.Background())
```

You can simple write to a BLOB with its id. You have to generate an ID yourself:

``` go
	err := store.Write(context.Background(), blobID, bytes.NewReader(data))
```

And read from it:

``` go
	rd, err := store.Read(context.Background(), blobID)
```

You have to close the returned reader. You can close it midway if you
are not interested in the whole stream.


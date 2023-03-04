# pkafka

This package implements a persistent writer for Kafka. It'll try to write to Kafka asynchronously, 
and if it fails, it'll retry until it succeeds. It'll persist the messages on either local or GCS 
disk, and will retry from there if it fails.
Upon initialization, it'll look for any messages that were not written to Kafka, and will try to
write them again.
It's designed to be used in a long-running process, and it'll keep retrying until it succeeds.

It uses the amazing franz-go package to do the heavy lifting.

## Usage

You initialize the write with the New() function. It takes a storage type, pkafka.LocalStorage 
or pkafka.GCSStorage, and a "bucketname". For GCS the bucketname is the name of the bucket, for local storage it is the 
path to the directory where the messages will be stores.

In addition, New takes a list of kgo.Option, which are passed to the franz-go client. It will add the option to enable 
transactions, which are needed for the persistent writer to work. Without it, we'd possibly change the messaging order.


## Example:

```go
package main

import (
	"context"
	"github.com/perbu/pkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"os/signal"
	"sync"
)

func main() {
	// Create a new writer
	writer, err := pkafka.New(pkafka.LocalStorage, "my-dir", kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}
	defer writer.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Start the writer
		err = writer.Run(ctx)
		if err != nil {
			panic(err)
		}
	}()
	// create a message:
	msg := kgo.Record{
		Topic: "my-topic",
        Value: []byte("my-value"),
    }
	// Write a message
	err = writer.PersistentProduce(msg)
    if err != nil {
		panic(err)
	}
    
	// Wait for the writer to finish (Ctrl+C)
	wg.Wait()
}
```
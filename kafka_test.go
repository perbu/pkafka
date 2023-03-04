package pkafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
	"testing"
	"time"
)

var brokers = []string{"localhost:9092"}

func TestNew(t *testing.T) {
	cl, err := New(LocalStorage, "bucket", kgo.AllowAutoTopicCreation(), kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	count := cl.GetMsgCount()
	fmt.Println("Message count:", count)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wg := sync.WaitGroup{}
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		errCh <- cl.Run(ctx)
	}()
	for i := 0; i < 10; i++ {
		s := fmt.Sprintf("test %d", i)
		msg := kgo.Record{
			Value: []byte(s),
			Topic: "test",
		}
		log.Println("Submitted to kafka: ", s)
		err := cl.PersistentProduce(msg)
		if err != nil {
			t.Errorf("PersistentProduce: %v", err)
		}
	}
	time.Sleep(time.Millisecond)
	cancel()
	wg.Wait()
	err = <-errCh
	if err != nil {
		t.Errorf("Run: %v", err)
	}

}

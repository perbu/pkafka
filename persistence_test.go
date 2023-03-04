package pkafka

import (
	"context"
	"fmt"
	"github.com/perbu/pkafka/local"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"testing"
	"time"
)

func newLocal(baseDir string) (*persistence, error) {
	gs, err := local.New(baseDir)
	if err != nil {
		return nil, fmt.Errorf("local init: %w", err)
	}
	p := persistence{
		persistor: gs,
		notebook:  "notebook",
		ready:     false,
	}
	return &p, nil
}

type TestStorage struct {
	Name string
	Age  int
}

func Test_persistence_persist(t *testing.T) {
	p, err := newLocal("testdir")
	if err != nil {
		t.Fatal(err)
	}
	objs := make([]*kgo.Record, 0, 10)
	for i := 10; i < 20; i++ {
		objs = append(objs, &kgo.Record{
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
			Timestamp: time.Now(),
			Topic:     "test",
		})
	}

	err = p.persist(objs)
	if err != nil {
		t.Fatal(err)
	}

	err = p.close()
	if err != nil {
		t.Fatal(err)
	}
	p = nil

	p, err = newLocal("testdir")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, err := p.read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("persistor closed")
	for obj := range ch {
		fmt.Printf("obj: '%s': %s\n", obj.Topic, obj.Key)
	}
	log.Println("done")
}

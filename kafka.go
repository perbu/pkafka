// Package pkafka provides a kafka producer that tries really hard never to lose messages.
// It'll retry messages that fail to send, and will buffer messages to disk, so we're sure we're not loosing them.
package pkafka

/*
 Messages flow like this:

 When the producer is called it'll just push the message into a buffered channel. This will make it async.
 On the other side of the channel there is a goroutine that will read the messages, in order, and send them to kafka.
 It'll batch up messages and send them to kafka every second.

 If a batch fails to send, it'll retry it for about 20 seconds. If it still fails it'll assume kafka is down and
 will switch to storing messages on GCS.

 It'll keep the first message in memory, and will keep on retrying endlessly.

 If it succeeds it'll remove the first message from disk and then re-ingest the next message from disk.

*/

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync/atomic"
	"time"
)

type Client struct {
	c            *kgo.Client
	failed       bool
	failedSince  time.Time
	lastAttempt  time.Time
	persistence  *persistence
	buffer       *buffer
	syncInterval time.Duration
	msgCount     *uint64
	bufferCh     chan *kgo.Record
	probeMessage *kgo.Record // this is a message we'll use to see if kafka is back up again, after being down.
	logger       *log.Logger
}

var (
	// ErrKafkaDown is returned when kafka is down.
	ErrKafkaDown = fmt.Errorf("kafka is down")
)

type StorageType int

const (
	LocalStorage StorageType = iota + 1
	GCSStorage
)

// New creates a new kafka client.
// It will apply the options you pass to it. kgo.TransactionalID is set to "perbu" by default to make
// the transactions work.
// See kgo.NewClient for more info on the options.
// It will ping kafka to make sure it's up and running and fail if it's not.
func New(sType StorageType, bucketName string, opts ...kgo.Opt) (*Client, error) {
	producerId := "perbu"
	opts = append(opts, kgo.TransactionalID(producerId))
	k, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("initializing kafka client: %w", err)
	}
	p, err := newPersistence(sType, bucketName)
	if err != nil {
		return nil, fmt.Errorf("initializing persistence: %w", err)
	}
	bu := &buffer{
		records: make([]*kgo.Record, 0),
	}
	logger := log.New(log.Writer(), "pkafka", log.Flags())
	s := Client{
		c:            k,
		persistence:  p,
		buffer:       bu,
		msgCount:     new(uint64),
		syncInterval: time.Second * 1,
		bufferCh:     make(chan *kgo.Record, 1000),
		failed:       false,
		logger:       logger,
	}

	return &s, nil
}

// Run starts the goroutine that will flush the buffer to kafka or GCS.
// this will reduce the need to locking, and will make the producer async.
func (c *Client) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()
	// defer c.c.Close()
	for {
		select {
		case <-ctx.Done():
			c.flush()
			return nil
		case msg := <-c.bufferCh:
			c.buffer.add(msg)
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *Client) PersistentProduce(msg kgo.Record) error {
	if msg.ProducerID != 0 {
		return fmt.Errorf("producer id must be 0")
	}
	c.bufferCh <- &msg
	return nil
}

// flush will attempt to flush the buffer to kakfa, or if kafka is down, to GCS.
// if that'll fail too then maybe we should abort?
func (c *Client) flush() {
	if c.buffer.size() == 0 {
		c.logger.Println("flush: nothing to flush")
		return
	}

	// If kafka is known to be down, we'll probe it every 30 seconds to see if it's back up:
	if c.failed && time.Since(c.lastAttempt) > time.Second*30 {
		c.lastAttempt = time.Now()
		err := c.probeKafka()
		if err == nil {
			c.failed = false
			c.logger.Println("flush: kafka is back up")
		}
	}

	// now try to flush to kafka if kafka is up.
	if !c.failed {
		err := c.flushToKafka()
		// if the flush succeeded, we'll just return, nothing more to do.
		if err == nil {
			return
		}
		c.failed = true
		c.failedSince = time.Now()
		c.logger.Println("flush: kafka went down")
	}

	c.flushToGCS()
}

// probeKafka will grab the first message from the buffer and send it to kafka.
func (c *Client) probeKafka() error {
	// if we don't have a probe message, we'll create one.
	if c.probeMessage == nil {
		c.probeMessage = &kgo.Record{
			Topic: "probe",
			Value: []byte("probe"),
		}
	}
	// if we have a probe message, we'll send it to kafka.
	res := c.c.ProduceSync(context.TODO(), c.probeMessage)
	err := res.FirstErr()
	if err != nil {
		return fmt.Errorf("probeKafka: %w", err)
	}
	return nil
}

// flushToKafka will flush the buffer to kafka.
// if kafka is down, it'll return an error and leave the messages in the buffer.
func (c *Client) flushToKafka() error {
	msgCount := c.buffer.size()
	c.logger.Printf("flushing %d msgs to kafka", msgCount)
	start := time.Now()
	defer c.logger.Printf("flushing to kafka took %v", time.Since(start))
	msgs := c.buffer.records
	retCh := make(chan error, msgCount)
	err := c.c.BeginTransaction()
	if err != nil {
		c.logger.Println("beginning transaction: ", err)
		return fmt.Errorf("beginning transaction: %w", err)
	}

	for i, msg := range msgs {
		c.logger.Println("submitting msg to kafka: ", i)
		c.c.Produce(context.TODO(), msg,
			func(r *kgo.Record, err error) { retCh <- err })
	}
	// all messages are sent, we're waiting for the return values.
	errs := make([]error, 0, msgCount)

	for i := 0; i < msgCount; i++ {
		err := <-retCh
		if err != nil {
			errs = append(errs, err)
			continue
		}
	}
	c.logger.Println("kafka: send: done")

	// if we had any errors, we'll ask kafka to rollback and just leave the messages in the buffer
	// to be flushed to GCS.
	if len(errs) != 0 {
		c.logger.Println("kafka: send: some messages failed. tx rollback.")
		rollback(context.Background(), c.c)
		return errors.Join(errs...)
	}

	// all messages were sent successfully.
	// we can commit the transaction.
	err = c.c.EndTransaction(context.Background(), kgo.TryCommit)
	if err != nil {
		c.logger.Println("committing tx: ", err)
		rollback(context.Background(), c.c)
		return fmt.Errorf("committing tx: %w", err)
	}
	c.buffer.clear()
	return nil
}

func rollback(ctx context.Context, client *kgo.Client) {
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Printf("error aborting buffered records: %v\n", err) // this only happens if ctx is canceled
		return
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}

func (c *Client) flushToGCS() {
	c.logger.Println("flushing to gcs")
	start := time.Now()
	defer c.logger.Printf("flushing to gcs took %v", time.Since(start))
	// we'll just write the messages to a file in GCS.
	err := c.persistence.persist(c.buffer.records)
	if err != nil {
		c.logger.Println("error writing to gcs: ", err)
		return
	}
	c.buffer.clear()
}

func (c *Client) GetMsgCount() uint64 {
	return atomic.LoadUint64(c.msgCount)
}

func (c *Client) Status() (string, error) {
	healthy := c.failed
	if !healthy {
		return fmt.Sprintf("Kafka down since %v", time.Since(c.failedSince)), ErrKafkaDown
	}
	return "Hunky dory", nil
}

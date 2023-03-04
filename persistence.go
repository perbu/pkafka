package pkafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/perbu/pkafka/gcs"
	"github.com/perbu/pkafka/local"
	"github.com/twmb/franz-go/pkg/kgo"
	"hash/crc32"
	"io"
	"log"
	"time"
)

type AbstractStorage interface {
	ListObjects(context.Context) ([]string, error)
	OpenObjectForWriting(context.Context, string) (io.WriteCloser, error)
	OpenObjectForReading(context.Context, string) (io.ReadCloser, error)
	DeleteObject(context.Context, string) error
}

type persistence struct {
	persistor AbstractStorage
	writer    io.WriteCloser
	notebook  string
	ready     bool
}

// PersistedMessage is the struct that is persisted to storage.
// It can be used to reconstruct the original kgo.Record, but suited for storage.
type PersistedMessage struct {
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Topic     string
}

func (m PersistedMessage) toRecord() *kgo.Record {
	return &kgo.Record{
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Topic:     m.Topic,
	}
}

func toPersistedMessage(msg *kgo.Record) PersistedMessage {
	return PersistedMessage{
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp,
		Topic:     msg.Topic,
	}
}

func newPersistence(sType StorageType, bucketName string) (*persistence, error) {

	var storage AbstractStorage
	var err error
	switch sType {
	case LocalStorage:
		log.Println("persistence init: local storage")
		storage, err = local.New(bucketName)
		if err != nil {
			return nil, fmt.Errorf("local init: %w", err)
		}
	case GCSStorage:
		log.Println("persistence init: GCS storage")
		storage, err = gcs.NewClient(context.TODO(), bucketName)
		if err != nil {
			return nil, fmt.Errorf("gcs init: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown storage type: %d", sType)

	}

	p := persistence{
		persistor: storage,
		notebook:  "notebook.wal",
		ready:     false,
	}
	// 10s timeout context:
	return &p, nil
}

// persist will ... persist the given messages. This usually means writing them to object storage.
func (p *persistence) persist(obj []*kgo.Record) error {
	// gob encode msg:
	// write to file
	if !p.ready {
		writer, err := p.persistor.OpenObjectForWriting(context.Background(), p.notebook)
		if err != nil {
			return fmt.Errorf("opening notebook: %w", err)
		}
		log.Println("opened notebook for writing")
		p.ready = true
		p.writer = writer
	}

	// transalte from kgo.Record to PersistedMessage:
	persisted := make([]PersistedMessage, 0, len(obj))
	for _, msg := range obj {
		persisted = append(persisted, toPersistedMessage(msg))
	}

	buffer := bytes.Buffer{}
	err := gob.NewEncoder(&buffer).Encode(persisted)
	if err != nil {
		return fmt.Errorf("gob encode: %w", err)
	}
	length := uint32(buffer.Len())
	checksum := crc32.ChecksumIEEE(buffer.Bytes())
	header := encodeHeader(length, checksum)
	n, err := p.writer.Write(header)
	if err != nil {
		return fmt.Errorf("writing header: %w", err)
	}
	if n != 8 {
		return fmt.Errorf("header short write")
	}
	n, err = p.writer.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("writing buffer: %w", err)
	}
	if n != int(length) {
		return fmt.Errorf("buffer short write")
	}
	return nil
}

func (p *persistence) close() error {
	log.Println("closing notebook")
	if !p.ready {
		return fmt.Errorf("persistence not ready, can't close")
	}
	err := p.writer.Close()
	if err != nil {
		return fmt.Errorf("closing writer: %w", err)
	}
	p.ready = false
	return nil
}

// read will open a reader to the given file and return a channel of messages.
func (p *persistence) read(ctx context.Context) (<-chan *kgo.Record, error) {
	log.Println("opening notebook for reading/recovery")
	start := time.Now()
	defer log.Printf("reading/recovery finished in %v", time.Since(start))
	if p.ready {
		return nil, fmt.Errorf("persistence already ready, can't replay")
	}
	reader, err := p.persistor.OpenObjectForReading(ctx, p.notebook)
	if err != nil {
		return nil, fmt.Errorf("opening notebook: %w", err)
	}
	ch := make(chan *kgo.Record, 10)
	go func() {
		defer close(ch)
		defer reader.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgs, err := readChunk(reader)
				if err != nil {
					if err == io.EOF {
						return
					}
					log.Printf("error reading chunk: %v", err)
					return
				}
				for _, m := range msgs {
					ch <- m
				}
			}
		}
	}()
	return ch, nil
}

func encodeHeader(length uint32, checksum uint32) []byte {
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], length)
	binary.BigEndian.PutUint32(header[4:8], checksum)
	return header
}

func decodeHeader(header []byte) (uint32, uint32) {
	length := binary.BigEndian.Uint32(header[0:4])
	checksum := binary.BigEndian.Uint32(header[4:8])
	return length, checksum
}

func readChunk(reader io.Reader) ([]*kgo.Record, error) {
	header := make([]byte, 8)
	n, err := reader.Read(header)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("error reading header: %w", err)
	}
	if n != 8 {
		return nil, fmt.Errorf("short read of header")
	}
	length, checksum := decodeHeader(header)
	buffer := make([]byte, length)
	n, err = reader.Read(buffer)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("unexpected EOF: %w", err)
		}
	}
	if n != int(length) {
		log.Printf("short read of buffer")
	}
	bufferCrc := crc32.ChecksumIEEE(buffer)
	if checksum != bufferCrc {
		return nil, fmt.Errorf("checksum mismatch, expected %x, got %x", checksum, bufferCrc)
	}
	msgs := make([]PersistedMessage, 0)
	err = gob.NewDecoder(bytes.NewReader(buffer)).Decode(&msgs)
	if err != nil {
		return nil, fmt.Errorf("gob decode: %w", err)
	}
	// translate from PersistedMessage to kgo.Record:
	returned := make([]*kgo.Record, 0, len(msgs))
	for _, msg := range msgs {
		returned = append(returned, msg.toRecord())
	}
	return returned, nil
}

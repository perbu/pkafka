package pkafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type buffer struct {
	oldest  time.Time
	records []*kgo.Record
}

func (b *buffer) size() int {
	return len(b.records)
}

func (b *buffer) add(msg ...*kgo.Record) {
	b.records = append(b.records, msg...)
	if len(b.records) == 1 {
		b.oldest = time.Now()
	}
}

// get returns the records.
// use under lock along with clear.
func (b *buffer) get() []*kgo.Record {
	return b.records
}
func (b *buffer) clear() {
	b.records = make([]*kgo.Record, 0)
	b.oldest = time.Time{}
}

func (b *buffer) age() time.Duration {
	return time.Since(b.oldest)
}

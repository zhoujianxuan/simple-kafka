package stdkafkago

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"sync"
	"testing"
)

var brokers = []string{"koreyoshi.work:9092"}

func TestStartConsume(t *testing.T) {
	c := &Consume{Name: "kafka_go_test_consume"}
	StartConsume(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    "kafka_go_test",
		GroupID:  "default",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	}, c.HandleKafkaMessage)
	wg := sync.WaitGroup{}
	wg.Add(1)

	wg.Wait()
}

type Consume struct {
	Name string
}

func (c *Consume) HandleKafkaMessage(m kafka.Message) error {
	fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	return nil
}

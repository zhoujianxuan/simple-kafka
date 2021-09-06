package core

import (
	"context"
	"log"
	"sync"
	"testing"

	"github.com/Shopify/sarama"
)

type TestMessage struct{}

func (m *TestMessage) GetTopic() string {
	return "demo_test"
}
func (m *TestMessage) GetData() []byte {
	return []byte("test")
}

var addr = []string{"<ip>"}

func TestNewAsyncProducerClient(t *testing.T) {
	client := NewAsyncProducerClient(addr)
	client.SendMessage(&TestMessage{})
	err := client.AsyncProducer.Close()
	if err != nil {
		return
	}
}

type ConsumerTestHandler struct {
	Name string
}

func (ConsumerTestHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerTestHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h ConsumerTestHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("%s Message topic:%q partition:%d offset:%d  value:%s\n", h.Name, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		// 手动确认消息
		sess.MarkMessage(msg, "")
	}
	return nil
}

func TestGroupConsumeClient_StartMonitor(t *testing.T) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V2_4_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	c := NewGroupConsumeClient("default", addr, []string{"demo_test"}, config)
	log.Println("start")
	w := sync.WaitGroup{}
	w.Add(1)
	err := c.StartMonitor(context.Background(), ConsumerTestHandler{Name: "test"})
	if err != nil {
		w.Done()
		return
	}
	w.Wait()
}

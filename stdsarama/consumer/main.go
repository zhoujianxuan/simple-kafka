package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

// kafka consumer

var addr = []string{"<ip>"}

func main() {
	wg := &sync.WaitGroup{}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = false
	config.Version = sarama.V2_4_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	consumer, err := sarama.NewConsumerGroup(addr, "default", config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	consume(&consumer, wg, "c8")
	wg.Wait()
}

func consume(group *sarama.ConsumerGroup, wg *sync.WaitGroup, name string) {
	fmt.Println(name + "start")
	ctx := context.Background()
	for {
		//topic := []string{"tiantian_topic1","tiantian_topic2"} 可以消费多个topic
		topics := []string{"demo_test"}
		handler := consumerGroupHandler{name: name}
		err := (*group).Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

type consumerGroupHandler struct {
	name string
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("%s Message topic:%q partition:%d offset:%d  value:%s\n", h.name, msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		// 手动确认消息
		sess.MarkMessage(msg, "")
	}
	return nil
}

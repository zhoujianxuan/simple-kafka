package core

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type Message interface {
	GetTopic() string
	GetName() string
	GetData() []byte
}

//AsyncProducerClient 生产者
type AsyncProducerClient struct {
	AsyncProducer sarama.AsyncProducer
}

func NewAsyncProducerClient(addr []string) *AsyncProducerClient {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 构造一个消息
	// 连接kafka
	client, err := sarama.NewAsyncProducer(addr, config)
	if err != nil {
		return nil
	}
	return &AsyncProducerClient{AsyncProducer: client}
}

// SendMessage 发送消息
func (client *AsyncProducerClient) SendMessage(msg Message) {
	client.AsyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: msg.GetTopic(),
		Headers: []sarama.RecordHeader{
			{Key: []byte("name"), Value: []byte(msg.GetName())},
		},
		Value: sarama.ByteEncoder(msg.GetData()),
	}
}

//GroupConsumeClient 消费者
type GroupConsumeClient struct {
	Addr    []string
	GroupID string
	Config  *sarama.Config
	Topics  []string
}

func NewGroupConsumeClient(groupID string, addr, topics []string, config *sarama.Config) *GroupConsumeClient {
	return &GroupConsumeClient{
		Addr:    addr,
		Topics:  topics,
		GroupID: groupID,
		Config:  config,
	}
}

// StartMonitor 开启
func (client *GroupConsumeClient) StartMonitor(ctx context.Context, handler sarama.ConsumerGroupHandler) error {
	consumer, err := sarama.NewConsumerGroup(client.Addr, client.GroupID, client.Config)
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println(r)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				break
			default:
			}
			err = (consumer).Consume(ctx, client.Topics, handler)
			if err != nil {
				log.Println(err)
				break
			}
		}
	}()
	return nil
}

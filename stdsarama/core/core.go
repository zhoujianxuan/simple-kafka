package core

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

type Message interface {
	GetTopic() string
	GetData() []byte
}

//AsyncProducerClient 生产者
type AsyncProducerClient struct {
	AsyncProducer sarama.AsyncProducer
}

//NewAsyncProducerClient 创建一个异步生产者
func NewAsyncProducerClient(addr []string) *AsyncProducerClient {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true // 成功交付的消息将在success channel返回

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
		Value: sarama.StringEncoder("msg.GetData()"),
	}
}

//GroupConsumeClient 消费者
type GroupConsumeClient struct {
	Addr    []string
	GroupID string
	Config  *sarama.Config
	Topics  []string
}

// NewGroupConsumeClient 新建一个消费端
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
	c, err := sarama.NewConsumerGroup(client.Addr, client.GroupID, client.Config)
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			_ = c.Close()
			if r := recover(); r != nil {
				log.Println(r)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			err = c.Consume(ctx, client.Topics, handler)
			if err != nil {
				log.Println(err)
				break
			}
		}
	}()
	return nil
}

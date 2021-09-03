package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

type Message struct {
	Action      string `json:"action"`
	LuckySpinID int32  `json:"luckySpinID"`
	UID         string `json:"uid"`
}

var addr = []string{"<ip>"}

func main() {
	// 连接kafka
	client, err := NewClient(addr)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer client.Close()
	// 发送消息
	client.Send("activity_lucky_spin", &Message{Action: "addTimes", LuckySpinID: 1, UID: "uid"})
}

type Client struct {
	ProducerClient sarama.AsyncProducer
}

func NewClient(addr []string) (*Client, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true // 成功交付的消息将在success channel返回

	// 构造一个消息
	// 连接kafka
	client, err := sarama.NewAsyncProducer(addr, config)
	if err != nil {
		return nil, err
	}
	return &Client{ProducerClient: client}, nil
}

func (c *Client) Send(topic string, v interface{}) {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	j, _ := json.Marshal(v)
	msg.Value = sarama.StringEncoder(j)
	c.ProducerClient.Input() <- msg
	// 等待结果
	select {
	case m := <-c.ProducerClient.Successes():
		log.Println(m)
	case m := <-c.ProducerClient.Errors():
		log.Println(m)
	default:
		log.Println("default")
	}
}

func (c *Client) Close() error {
	err := c.ProducerClient.Close()
	if err != nil {
		return err
	}
	return nil
}

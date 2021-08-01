package stdkafkago

import (
	"context"
	"fmt"
	kafka "github.com/segmentio/kafka-go"
	"log"
	"testing"
)

func TestGetKafkaWriter(t *testing.T) {
	kafkaWriter := GetKafkaWriter("koreyoshi.work:9092", "kafka_go_test")
	defer kafkaWriter.Close()
	msgs := make([]kafka.Message, 0, 1000)
	for i := 1; i <= cap(msgs); i++ {
		msg := kafka.Message{
			Value: []byte(fmt.Sprintf("hello kafka %d", i)),
		}
		msgs = append(msgs, msg)
	}
	err := kafkaWriter.WriteMessages(context.Background(), msgs...)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("发送成功")
}

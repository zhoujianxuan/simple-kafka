package stdkafkago

import (
	"context"
	kafka "github.com/segmentio/kafka-go"
	"log"
)

func GetKafkaReader(brokers []string, groupID, topic string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return r
}

func StartConsume(config kafka.ReaderConfig, f func(message kafka.Message) error) {
	r := kafka.NewReader(config)
	go func() {
		if re := recover(); re != nil {
			log.Fatalln(re)
		}
		defer r.Close()
		for {
			msg, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Fatalln(err)
			}
			err = f(msg)
			if err != nil {
				log.Fatalln(err)
			}
		}
	}()
}

package main

import (
	"context"
	"fmt"
	"log"
	"simple-kafka/reader"
)

func main() {
	// get kafka reader using environment variables.
	//kafkaURL := os.Getenv("kafkaURL")
	//topic := os.Getenv("topic")
	//groupID := os.Getenv("groupID")
	kafkaURL := "192.168.3.113:9092,192.168.3.114:9092,192.168.3.115:9092"
	topic := "activity_lucky_spin"
	groupID := "default"

	r := reader.GetKafkaReader(kafkaURL, topic, groupID)

	defer r.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer := NewConsumer()

	topics := []string{"teste"}

	consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err != nil {
			log.Fatalf("error to read menssage %v", err)
		}

		fmt.Println(string(msg.Value), " => ", msg.TopicPartition.String())
	}

}

func NewConsumer() *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka-estudo_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
	})

	if err != nil {
		log.Fatalf("cannot create kafka consumer %v", err)
	}

	return c
}

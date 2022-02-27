package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()

	err := Publish(
		"msg-01",
		"teste",
		nil,
		producer,
	)

	producer.Flush(1000)

	if err != nil {
		log.Fatalf("cannot publish message: %v", err)
	}

	fmt.Println("message published")
}

func NewKafkaProducer() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka-estudo_kafka_1:9092",
	})

	if err != nil {
		log.Fatalf("cannot create kafka producer: %v", err)
	}

	return p
}

func Publish(value, topic string, key []byte, producer *kafka.Producer) error {
	msg := &kafka.Message{
		Value: []byte(value),
		Key:   key,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}

	err := producer.Produce(msg, nil)

	return err
}

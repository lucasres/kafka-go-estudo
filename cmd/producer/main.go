package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	var asyncF bool

	flag.BoolVar(&asyncF, "async", false, "make mod async")

	flag.Parse()

	if asyncF {
		Async()
	} else {
		Sync()
	}

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

func Publish(value, topic string, key []byte, producer *kafka.Producer, deliveryChan chan kafka.Event) error {
	msg := &kafka.Message{
		Value: []byte(value),
		Key:   key,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
	}

	err := producer.Produce(msg, deliveryChan)

	return err
}

func Sync() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()

	err := Publish(
		"msg-01",
		"teste",
		nil,
		producer,
		deliveryChan,
	)

	if err != nil {
		log.Fatalf("cannot publish message: %v", err)
	}

	e := <-deliveryChan

	msg := e.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		log.Fatalf("cannot publish message: %v", msg.TopicPartition.Error)
	}

	fmt.Println("message published: " + msg.TopicPartition.String())
}

func Async() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()

	err := Publish(
		"msg-01",
		"teste",
		nil,
		producer,
		deliveryChan,
	)

	if err != nil {
		log.Fatalf("cannot publish message: %v", err)
	}

	go DeliveryReport(deliveryChan)

	producer.Flush(2000)
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Fatalf("cannot publish message: %v", ev.TopicPartition.Error)
			}

			fmt.Println("message published: " + ev.TopicPartition.String())
		}
	}
}

package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	ch := make(chan kafka.Event)
	fmt.Println("Conectando...")
	p, err := newKafkaProducer()
	if err != nil {
		panic(err)
	}

	fmt.Println("Publicando mensagem...")
	err = publishMensage(
		p,
		"order",
		"#1",
		`{"orderId": "1", "userId":"1", "products": [{"id": 1, "qtd": 1}]}`,
		ch,
	)
	if err != nil {
		panic(err)
	}
	// espera o retorno do chan
	result := <-ch
	fmt.Println("Resultado:")
	fmt.Println(result.String())
}

func newKafkaProducer() (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "kafka-estudo_kafka_1:9092",
	})
}

func publishMensage(
	producer *kafka.Producer,
	topic, key, payload string,
	ch chan kafka.Event,
) error {
	msg := &kafka.Message{
		Value: []byte(payload),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: []byte(key),
	}

	return producer.Produce(msg, ch)
}

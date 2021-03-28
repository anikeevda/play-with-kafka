package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     "broker:9092",
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 1, //VERY IMPORTANT,
		"compression.type":                      "none",
		"retries":                               3,
		"client.id":                             "producer",
		"enable.idempotence":                    true,       //VERY IMPORTANT,
		"auto.offset.reset":                     "earliest", //VERY IMPORTANT
	})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	for _, word := range []string{"Welcome", "to"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, //Partition any IS IMPORTANT TO USE HASH ALGORITHM ON KEY
			Key:            []byte("messageId"),
			Value:          []byte(word),
		}, nil)
	}

	for _, word := range []string{"the", "Confluent"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("3920892"),
			Value:          []byte(word),
		}, nil)
	}

	for _, word := range []string{"Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("3232dlsk392dskjd1rgf"),
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}

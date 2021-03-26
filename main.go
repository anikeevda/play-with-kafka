package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

func main() {
	//p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "broker:9092"})
	//if err != nil {
	//	panic(err)
	//}
	//
	//defer p.Close()
	//
	//// Delivery report handler for produced messages
	//go func() {
	//	for e := range p.Events() {
	//		switch ev := e.(type) {
	//		case *kafka.Message:
	//			if ev.TopicPartition.Error != nil {
	//				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
	//			} else {
	//				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
	//			}
	//		}
	//	}
	//}()
	//
	//// Produce messages to topic (asynchronously)
	//topic := "myTopic"
	//for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
	//	p.Produce(&kafka.Message{
	//		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	//		Value:          []byte(word),
	//	}, nil)
	//}
	//
	//// Wait for message deliveries before shutting down
	//p.Flush(15 * 1000)
	//log.Println("here1")
	//c1 := make(chan string, 1)
	//go func() {
	//	time.Sleep(time.Millisecond * 500)
	//	c1 <- "result 1"
	//}()
	//for {
	//	select {
	//	case res := <-c1:
	//		fmt.Println(res)
	//		return
	//	default:
	//		fmt.Println("next")
	//		time.Sleep(50 * time.Millisecond)
	//	}
	//}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "broker",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"myTopic"}, nil)

	for {
		select {
		case <-time.After(time.Second * 4):
			fmt.Println("no messages")
			return
		default:
			msg, err := c.ReadMessage(time.Second * 5)
			if err == nil {
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			}
		}

	}

	c.Close()
}

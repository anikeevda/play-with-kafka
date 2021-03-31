package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var run = true

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     "broker",
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 1, //VERY IMPORTANT,
		"client.id":                             "producer",
		"enable.idempotence":                    true, //VERY IMPORTANT
	})
	if err != nil {
		panic(err)
	}

	topic := "myTopic"

	// For signalling termination from main to go-routine
	// For signalling termination from main to go-routine
	termChan := make(chan bool, 1)
	// For signalling that termination is done from go-routine to main
	doneChan := make(chan bool, 1)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Go routine for serving the events channel for delivery reports and error events.
	go func() {
		doTerm := false
		for !doTerm {
			select {
			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					// Message delivery report
					m := ev
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}

				case kafka.Error:
					// Generic client instance-level errors, such as
					// broker connection failures, authentication issues, etc.
					//
					// These errors should generally be considered informational
					// as the underlying client will automatically try to
					// recover from any errors encountered, the application
					// does not need to take action on them.
					//
					// But with idempotence enabled, truly fatal errors can
					// be raised when the idempotence guarantees can't be
					// satisfied, these errors are identified by
					// `e.IsFatal()`.

					e := ev
					if e.IsFatal() {
						// Fatal error handling.
						//
						// When a fatal error is detected by the producer
						// instance, it will emit kafka.Error event (with
						// IsFatal()) set on the Events channel.
						//
						// Note:
						//   After a fatal error has been raised, any
						//   subsequent Produce*() calls will fail with
						//   the original error code.
						fmt.Printf("FATAL ERROR: %v: terminating\n", e)
						run = false
					} else {
						fmt.Printf("Error: %v\n", e)
					}

				default:
					fmt.Printf("Ignored event: %s\n", ev)
				}

			case <-termChan:
				doTerm = true
			}
		}

		fmt.Println("prepare to end")
		close(doneChan)
	}()

	msgcnt := 0

	for run == true {
		select {
		case <-sigchan:
			fmt.Println("Caught signal")
			run = false
		default:
			value := fmt.Sprintf("Go Idempotent Producer example, message #%d", msgcnt)

			// Produce message.
			// This is an asynchronous call, on success it will only
			// enqueue the message on the internal producer queue.
			// The actual delivery attempts to the broker are handled
			// by background threads.
			// Per-message delivery reports are emitted on the Events() channel,
			// see the go-routine above.
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte("dsdsd"),
				Value:          []byte(value),
			}, nil)

			if err != nil {
				fmt.Printf("Failed to produce message: %v\n", err)
			}

			msgcnt++

			// Since fatal errors can't be triggered in practice,
			// use the test API to trigger a fabricated error after some time.
			if msgcnt == 13 {
				p.TestFatalError(kafka.ErrOutOfOrderSequenceNumber, "Testing fatal errors")
			}

			time.Sleep(500 * time.Millisecond)

		}
	}

	// Clean termination to get delivery results
	// for all outstanding/in-transit/queued messages.
	fmt.Printf("Flushing outstanding messages\n")
	p.Flush(15 * 1000)

	termChan <- true
	// wait for go-routine to terminate
	<-doneChan

	fatalErr := p.GetFatalError()

	p.Close()

	// Exit application with an error (1) if there was a fatal error.
	if fatalErr != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

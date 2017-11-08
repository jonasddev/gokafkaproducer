package main

import (
	"github.com/Shopify/sarama"
	"log"
	"bufio"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		panic("Usage: gokafkaproducer <kafkaserver:port> <topic>"	)
	}
	server := os.Args[1]
	topic := os.Args[2]
	producer, err := sarama.NewSyncProducer([]string{server}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	reader := bufio.NewReader(os.Stdin)
	for true {
		var text string
		text, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(text)}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("FAILED to send message: %s\n", err)
			break
		}
	}
}

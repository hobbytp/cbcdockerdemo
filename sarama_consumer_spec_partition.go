package main


import (
	"fmt"
	"os"
	"os/signal"
	"github.com/Shopify/sarama"
)

//read specific partition to get msg.
func readByPartition() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V1_1_0_0

	// Specify brokers address. This is default one
	brokers := []string{"localhost:9092"}

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "hobtest"

  //The partition number is 0, the consumer will only read from this partition
	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	//consumer, err := master.ConsumePartition(topic, 1, sarama.OffsetOldest)

	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Println("Received messages: ", string(msg.Key), string(msg.Value))

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}


func main(){
	readByPartition()

}

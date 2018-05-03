package main

import (
	"fmt"
	"os"
	"os/signal"
	"time"
	"strconv"
	"github.com/Shopify/sarama"
)

func main() {

	// Setup configuration
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Version = sarama.V1_1_0_0


	// The level of acknowledgement reliability needed from the broker.
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{"localhost:9092"}



	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)


	var enqueued1, enqueued2, errors int
	var ind int
	ind = 0
	doneCh := make(chan struct{})
	go func() {
		for {
            if ind >= 10 {
            	break
			}
			ind ++
     
			time.Sleep(500 * time.Millisecond)

			strTime := strconv.Itoa(int(time.Now().Unix()))
			keyVal := sarama.StringEncoder(strTime)
       
      //same topic with different message key would goto different partition.
			msg := &sarama.ProducerMessage{
				Topic: "hobtest",
				Key:   sarama.StringEncoder("1234"),
				Value: sarama.StringEncoder("Something Cool: " + "Subtopic-1" + keyVal),
			}
			msg2 := &sarama.ProducerMessage{
				Topic: "hobtest",

				Key:   sarama.StringEncoder("56789"),
				Value: sarama.StringEncoder("Something Warm: " + "Subtopic-2" + keyVal),
			}
			//partition, offset, err := producer.SendMessage(message)  //one method

			select {
			case producer.Input() <- msg:
				enqueued1++
				fmt.Println("Produce message: "+ "Subtopic-1" )
			case producer.Input() <- msg2:
				enqueued2++
				fmt.Println("Produce message: "+ "Subtopic-2")
			case err := <-producer.Errors():
				errors++
				fmt.Println("Failed to produce message:", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

	fmt.Println("Processed", enqueued1, "messages")
	fmt.Println("Processed", enqueued2, "messages")
}

package main

import (
	"github.com/Shopify/sarama"
	"fmt"
	"time"
)

func main() {
	createtopics()
}

func createtopics() {
	var connected bool
	var err error

	// Setup configuration
	config := sarama.NewConfig()
	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	config.Version = sarama.V1_1_0_0

	// The level of acknowledgement reliability needed from the broker.
	config.Producer.RequiredAcks = sarama.WaitForAll

	//We would get kafka service VIP only, so we use it directly and use it get broker number which
	//can be used to design the replica number.
	//var brokers = []string{"localhost:9092","localhost:9093"}
	var brokers = []string{"localhost:9092"} //Use only one broker info is enough
	client, err := sarama.NewClient(brokers, config)

	if err != nil {
		fmt.Println("New client not created", err)
		panic(err)
	}

	//it can list all running brokers by using Client.
	//The replica number shall less/equal than the brokers' number
	var brokerNum = len(client.Brokers())
	fmt.Println("The brokers number is: ", brokerNum)
	//time.Sleep(time.Second * 5)

	topicReq := &sarama.CreateTopicsRequest{}
	topicReq.TopicDetails = make(map[string]*sarama.TopicDetail)
	topicReq.TopicDetails["hobtest"] = &sarama.TopicDetail{NumPartitions: 2, ReplicationFactor: int16(brokerNum)}

	/* TBD: full Topic detail is as below, check assignment later
	req := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			"hobbytest": {
				NumPartitions:     2,
				ReplicationFactor: 2,
				ReplicaAssignment: map[int32][]int32{
					0: []int32{0,1},
					1: []int32{0,1},
				},
				ConfigEntries: map[string]*string{
					"retention.ms": &retention,
				},
			},
		},
		Timeout: 100 * time.Millisecond,
	}
	*/

	
	client.Brokers()[0].Open(config) //
	connected, err = client.Brokers()[0].Connected()
	if err != nil {
		panic(err)
	}

	if connected {		
		rsp, err := client.Brokers()[0].CreateTopics(topicReq)
		if err != nil {
			panic(err)
		}
		fmt.Println(rsp.TopicErrors)
	} else {
		fmt.Println("client.Brokers()[0].Connected() fail!")
	}
}

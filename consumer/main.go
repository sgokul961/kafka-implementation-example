package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	//create a new consumer and start it
	topic := "coffer_orders"
	msgCnt := 0
	worker, err := ConncetToConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	fmt.Println("consumer started")
	//handle OS signals used to stop  the process
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	//create a Goroutine  to run the consumer worker
	donech := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCnt++
				fmt.Printf("Recived Order Count (%d):| Topic (%s) |message (%s) \n", msgCnt, string(msg.Topic), string(msg.Value))
				order := string(msg.Value)
				fmt.Printf("Brewing coffe for orders: %s\n", order)
			case <-sigchan:
				fmt.Println("Inturrupt is detected")
				donech <- struct{}{}
			}
		}
	}()
	<-donech
	fmt.Println("processed", msgCnt, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}

	// close the consumer on exit

}
func ConncetToConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}

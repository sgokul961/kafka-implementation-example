package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func main() {
	http.HandleFunc("/order", placeorder)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
func ConncetToProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, config)
}
func PushOrderToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}
	producer, err := ConncetToProducer(brokers)

	if err != nil {
		return err
	}
	defer producer.Close()

	//creaye a kafka message

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	//send this message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("Order is stored in topic(%s)/partition (%d)/offset(%d)\n",
		topic, partition, offset)
	return nil
}
func placeorder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request Methord", http.StatusBadRequest)
		return
	}
	order := new(Order)
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return

	}
	//convert body into bytes
	orderBytes, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	//send the message
	err = PushOrderToQueue("coffer_orders", orderBytes)

	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//repond back to user
	response := map[string]interface{}{
		"Success": true,
		"Msg":     "Order for " + order.CustomerName + " placed successfully!",
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, "Error Placing Order", http.StatusInternalServerError)
		return
	}
}

//use this curl to send the message
//curl -X POST http://localhost:8080/order   -H "Content-Type: application/json"   -d '{"customer_name": "greshma", "coffee_type": "cappuchino"}'

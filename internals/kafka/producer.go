package kafka

import (
	"datagremlin/internals/datamodels"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"

	getenv "github.com/joho/godotenv"
)

type KafkaEvent struct {
	Operation string                 `json:"operation"`
	Schema    string                 `json:"schema"`
	Table     string                 `json:"table"`
	Data      map[string]interface{} `json:"data"`
}

var TopicEventMap = map[string]string{
	"UPDATE_RECORD": "update_topic_dev",
	"DELETE_RECORD": "delete_topic_dev",
	"INSERT_RECORD": "insert_topic_dev",
}

func CreateProducer() (sarama.SyncProducer, error) {
	_ = getenv.Load()

	brokers := []string{fmt.Sprintf("%s:%s", os.Getenv("KAFKA_HOST"), os.Getenv("KAFKA_PORT"))}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create a producer: %v", err)
		return nil, err
	}

	return producer, nil
}

func SendStringMessage(producer sarama.SyncProducer, message string, topic string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
		return err
	}

	log.Printf("Message sent to partition %d at offset %d", partition, offset)
	return nil
}

func SendKafkaEventMessage(producer sarama.SyncProducer, message models.KafkaEvent) error {
	dest_topic, ok := TopicEventMap[message.Operation]
	if !ok {
		return fmt.Errorf("[ERROR] no kafka topic mapped for this operation: %s", message.Operation)
	}

	// serialize the struct to JSON
	valueBytes, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal kafka event message: %w", err)
	}

	// send the marshaled bytes
	msg := &sarama.ProducerMessage{
		Topic: dest_topic,
		Value: sarama.ByteEncoder(valueBytes),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("[DEBUG-KAFKA] failed to send message: %v", err)
		return err
	}

	log.Printf("Kafka Event sent to topic=%s partition=%d at offset:%d\n", dest_topic, partition, offset)
	return nil
}

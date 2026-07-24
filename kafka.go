package eg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	p *kafka.Producer
}

func NewKafkaProducer(brokers, username, password string) *KafkaProducer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"acks":               "all",
		"enable.idempotence": true,
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "SCRAM-SHA-256",
		"sasl.username":      username,
		"sasl.password":      password,
	})
	if err != nil {
		log.Fatalf("producer create error: %v", err)
	}

	return &KafkaProducer{
		p: p,
	}
}

func (kp *KafkaProducer) Produce(topic, key string, value any) error {
	v, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Marshal: %w", err)
	}

	err = kp.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(key),
		Value: v,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to produce: %w", err)
	}

	return nil
}

// ----------

type KafkaConsumer struct {
	c *kafka.Consumer
}

func NewKafkaConsumer(brokers, groupID, username, password string) *KafkaConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
		"security.protocol":  "SASL_PLAINTEXT",
		"sasl.mechanism":     "SCRAM-SHA-256",
		"sasl.username":      username,
		"sasl.password":      password,
	})
	if err != nil {
		log.Fatalf("consumer create error: %v", err)
	}

	return &KafkaConsumer{
		c: c,
	}
}

type KafkaMessage struct {
	Key   string
	Value any
}

func (kp *KafkaConsumer) Consume(ctx context.Context, topic string) (chan *KafkaMessage, error) {
	defer kp.c.Close()

	if err := kp.c.SubscribeTopics([]string{topic}, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe topic: %w", err)
	}

	output := make(chan *KafkaMessage)

	go func() {
		run := true
		for run {
			select {
			case <-ctx.Done():
				close(output)
				run = false
			default:
				ev := kp.c.Poll(100)
				if ev == nil {
					continue
				}

				switch e := ev.(type) {
				case *kafka.Message:
					log.Printf("received: topic=%s partition=%d offset=%d\n",
						*e.TopicPartition.Topic,
						e.TopicPartition.Partition,
						e.TopicPartition.Offset,
					)

					if _, err := kp.c.CommitMessage(e); err != nil {
						log.Printf("commit error: %v", err)
					}

					output <- &KafkaMessage{
						Key:   string(e.Key),
						Value: e.Value,
					}

				case kafka.Error:
					log.Printf("kafka error: %v", e)

				default:
					// ignore other events
				}
			}
		}
	}()

	return output, nil
}

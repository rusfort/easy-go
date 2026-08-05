package eg

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	kafka "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaProducer struct {
	p *kafka.Client
}

func NewKafkaProducer(brokers, username, password string) *KafkaProducer {
	saslMechanism := scram.Auth{
		User: username,
		Pass: password,
	}.AsSha256Mechanism()

	cl, err := kafka.NewClient(
		kafka.SeedBrokers(strings.Split(brokers, ",")...),
		kafka.SASL(saslMechanism),
		kafka.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("producer create error: %v", err)
	}

	return &KafkaProducer{p: cl}
}

func (kp *KafkaProducer) Produce(ctx context.Context, topic, key string, value any) error {
	v, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Marshal: %w", err)
	}

	record := &kafka.Record{Topic: topic, Key: []byte(key), Value: v}
	res := kp.p.ProduceSync(ctx, record)
	if err = res.FirstErr(); err != nil {
		return fmt.Errorf("ProduceSync: %w", err)
	}

	return nil
}

func (kp *KafkaProducer) Close() {
	kp.p.Close()
}

// ----------

type KafkaConsumer struct {
	c *kafka.Client
}

func NewKafkaConsumer(brokers, groupID, topic, username, password string) *KafkaConsumer {
	saslMechanism := scram.Auth{
		User: username,
		Pass: password,
	}.AsSha256Mechanism()

	cl, err := kafka.NewClient(
		kafka.SeedBrokers(strings.Split(brokers, ",")...),
		kafka.ConsumerGroup(groupID),
		kafka.ConsumeTopics([]string{topic}...),
		kafka.DisableAutoCommit(),
		kafka.SASL(saslMechanism),
	)
	if err != nil {
		log.Fatalf("consumer create error: %v", err)
	}
	return &KafkaConsumer{c: cl}
}

type KafkaMessage struct {
	Key   string
	Value any
}

func (kp *KafkaConsumer) Consume(ctx context.Context) (*Chan[*KafkaMessage], error) {
	defer kp.c.Close()

	output := NewChan[*KafkaMessage]()

	go func() {
		run := true
		for run {
			select {
			case <-ctx.Done():
				output.Close()
				run = false
			default:
				fetches := kp.c.PollFetches(ctx)
				if errs := fetches.Errors(); len(errs) > 0 {
					log.Printf("Poll errors: %v", errs)
				}

				iter := fetches.RecordIter()
				var records []*kafka.Record
				for !iter.Done() {
					record := iter.Next()
					records = append(records, record)
					output.Write(&KafkaMessage{
						Key:   string(record.Key),
						Value: record.Value,
					})
				}

				if err := kp.c.CommitRecords(ctx, records...); err != nil {
					log.Printf("Failed to commit records: %v", err)
				}
			}
		}
	}()

	return output, nil
}

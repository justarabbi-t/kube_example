package services

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var kConfig kafka.ConfigMap = kafka.ConfigMap{
	"bootstrap.servers": "docker.rabbit.home:9092",
	"client.id":         "myProducer",
	"group.id":          "myConsumer",
	"acks":              "all",
}

type ProducerWithChan[T any] struct {
	Producer *kafka.Producer
	Channel  chan T
	Ctx      context.Context
	Topic    Topic
}

func getAppUuid(n string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s", n, uuid.New()))
}

func NewCfgMap(clientId, groupId string) kafka.ConfigMap {
	cfg := maps.Clone(kConfig)
	if clientId != "" {
		cfg["client.id"] = getAppUuid(clientId)
	} else {
		if s, ok := cfg["client.id"].(string); ok {
			cfg["client.id"] = getAppUuid(s)
		} else {
			cfg["client.id"] = getAppUuid("Default")
		}
	}
	if groupId != "" {
		cfg["group.id"] = getAppUuid(groupId)
	} else {
		if s, ok := cfg["group.id"].(string); ok {
			cfg["group.id"] = getAppUuid(s)
		} else {
			cfg["group.id"] = getAppUuid("Default")
		}
	}
	return cfg
}

func NewProducerWithJsonChan[T any](kafkaCfg kafka.ConfigMap, topic Topic, ctx context.Context) (*ProducerWithChan[T], error) {
	c := make(chan T, 3)
	producer, err := kafka.NewProducer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("NewProducerWithJsonChan %w", err)
	}
	return &ProducerWithChan[T]{
		Channel:  c,
		Producer: producer,
		Ctx:      ctx,
		Topic:    topic,
	}, nil
}

type ConsumerWithChan[T any] struct {
	Consumer *kafka.Consumer
	Channel  chan T
	Ctx      context.Context
	Topic    Topic
}

func NewConsumerWithJsonChan[T any](kafkaCfg kafka.ConfigMap, topic Topic, ctx context.Context) (*ConsumerWithChan[T], error) {
	c := make(chan T, 3)
	Consumer, err := kafka.NewConsumer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("NewConsumerWithJsonChan %w", err)
	}
	return &ConsumerWithChan[T]{
		Channel:  c,
		Consumer: Consumer,
		Ctx:      ctx,
		Topic:    topic,
	}, nil
}
func (c *ConsumerWithChan[T]) Watch(e chan<- error) {
	defer c.Consumer.Close()
	c.Consumer.Subscribe(c.Topic.String(), nil)

ConsumerLoop:
	for {
		select {
		case <-c.Ctx.Done():
			fmt.Println("ctx.Done Exiting ProducerLoop")
			break ConsumerLoop
		default:
			fmt.Println("here1")
			event := c.Consumer.Poll(100)
			fmt.Println("here2")
			if m, ok := event.(*kafka.Message); ok {
				fmt.Println("here3")
				fmt.Printf("Message on %s:\n%s\n", m.TopicPartition, string(m.Value))
				msg := *new(T)
				err := json.Unmarshal(m.Value, msg)
				if err != nil {
					e <- fmt.Errorf("ConsumerWithChan.Watch ConsumerLoop %w", err)
				}
				c.Channel <- msg
			} else if err, ok := event.(kafka.Error); ok {
				fmt.Println("here4")
				fmt.Printf("Error: %v\n", err)
				e <- fmt.Errorf("ConsumerWithChan.Watch ConsumerLoop %w", err)
			}
		}
		fmt.Println("here5")
	}
}

func (p *ProducerWithChan[T]) Watch(e chan<- error) {
	defer p.Producer.Close()
ProducerLoop:
	for {
		select {
		case anAction := <-p.Channel:
			msg, err := json.Marshal(anAction)
			if err != nil {
				e <- fmt.Errorf("warning err: %w", err)
				continue ProducerLoop
			}
			topic := p.Topic.String()
			p.Producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value: msg,
			}, nil)
		case <-p.Ctx.Done():
			fmt.Println("ctx.Done Exiting ProducerLoop")
			break ProducerLoop
		}
	}
}

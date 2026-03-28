package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

var kConfig kafka.ConfigMap = kafka.ConfigMap{
	"bootstrap.servers": "docker.rabbit.home:9092",
	"client.id":         "myProducer",
	"acks":              "all",
}

type ProducerWithChan struct {
	Producer *kafka.Producer
	channel  <-chan map[string]string
	topic    string
	ctx      context.Context
}

func getAppUuid(n string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s", n, uuid.New()))
}

func getCfgMap(n string) kafka.ConfigMap {
	cfg := maps.Clone(kConfig)
	if n != "" {
		cfg["client.id"] = getAppUuid(n)
	} else {
		if s, ok := cfg["client.id"].(string); ok {
			cfg["client.id"] = getAppUuid(s)
		} else {
			cfg["client.id"] = getAppUuid("Default")
		}
	}
	return cfg
}
func (p *ProducerWithChan) Watch(e chan error) {
	watchKafkaTalker(p, e)
}

func NewProducerWithChan(c <-chan map[string]string, appName string, topic string, ctx context.Context) *ProducerWithChan {
	cfg := getCfgMap(appName)
	producer, err := kafka.NewProducer(&cfg)
	if err != nil {
		return nil
	}
	return &ProducerWithChan{
		channel:  c,
		Producer: producer,
		topic:    topic,
		ctx:      ctx,
	}
}

type ConsumerWithChan struct {
	Consumer *kafka.Consumer
	channel  chan<- map[string]string
	topic    string
	ctx      context.Context
}

func (c *ConsumerWithChan) Watch(e chan error) {
	watchKafkaTalker(c, e)
}

func NewConsumerWithChan(c chan<- map[string]string, appName string, topic string, ctx context.Context) *ConsumerWithChan {
	cfg := getCfgMap(appName)
	Consumer, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil
	}
	return &ConsumerWithChan{
		channel:  c,
		Consumer: Consumer,
		topic:    topic,
		ctx:      ctx,
	}
}

type Watcher interface {
	watch(e chan error)
}

func watchKafkaTalker(talker any, e chan error) {
	switch t := talker.(type) {
	case ConsumerWithChan:
		t.watch(e)
		e <- nil
	case ProducerWithChan:
		t.watch(e)
		e <- nil
	default:
		e <- errors.New("watchKafkaTalker talker is unknown type")
	}
}
func (c *ConsumerWithChan) watch(e chan error) {
	c.readKafkaWriteChan(e)
}

func (c *ConsumerWithChan) readKafkaWriteChan(e chan error) {
	return func() error { return errors.New("unimplemented") }()
}

func (p *ProducerWithChan) watch(e chan error) {
	p.readChanWriteKafka(e)
}
func (p *ProducerWithChan) readChanWriteKafka(e chan error) {
ProducerLoop:
	for {
		select {
		case anAction := <-p.channel:
			msg, err := json.Marshal(anAction)
			if err != nil {
				fmt.Printf("warning err: %s", err)
				continue ProducerLoop
			}
			p.Producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &p.topic,
					Partition: kafka.PartitionAny,
				},
				Value: []byte(msg),
			}, nil)
		case <-p.ctx.Done():
			fmt.Println("ctx.Done Exiting ProducerLoop")
			break ProducerLoop
		}
	}
}

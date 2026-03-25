package services

import (
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

type GenericTopic int

const (
	Topic1 GenericTopic = iota
	Topic2
	Topic3
)

func (g GenericTopic) String() string {
	return [...]string{"Topic1", "Topic2", "Topic3"}[g]
}

type EnumLike interface {
	~int | ~string
}

type ProducerWithChan[T1 any, T2 EnumLike] struct {
	Producer *kafka.Producer
	channel  <-chan T1
	topic    T2
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

func NewProducerWithChan[T1 any, T2 EnumLike](c <-chan T1, appName string, topic T2) *ProducerWithChan[T1, T2] {
	cfg := getCfgMap(appName)
	producer, err := kafka.NewProducer(&cfg)
	if err != nil {
		return nil
	}
	return &ProducerWithChan[T1, T2]{
		channel:  c,
		Producer: producer,
		topic:    topic,
	}
}

type ConsumerWithChan[T1 any, T2 EnumLike] struct {
	Consumer *kafka.Consumer
	channel  chan<- T1
	topic    T2
}

func NewConsumerWithChan[T1 any, T2 EnumLike](c chan<- T1, appName string, topic T2) *ConsumerWithChan[T1, T2] {
	cfg := getCfgMap(appName)
	Consumer, err := kafka.NewConsumer(&cfg)
	if err != nil {
		return nil
	}
	return &ConsumerWithChan[T1, T2]{
		channel:  c,
		Consumer: Consumer,
		topic:    topic,
	}
}
func main() {
	c := make(chan string, 1)
	d1 := NewConsumerWithChan[string](c, "")
	d2 := NewProducerWithChan[string](c, "")

}

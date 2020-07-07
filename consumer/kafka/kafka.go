package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	proto "github.com/golang/protobuf/proto"
	flow "github.com/introvertautist/kafka_consumer/pb-ext"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	strategySticky      = "sticky"
	strategyRoundrobin  = "roundrobin"
	strategyRage        = "range"
	workStrategyForever = "forever"
)

// AppConfig - Structure with application configuration data
type AppConfig struct {
	KafkaBrokers string
	Group        string
	Version      string
	Topics       string
	Assignor     string
	Strategy     string
	Oldest       bool
}

type FlowToExport struct {
	TimeFlowStart time.Time                 `json:"start"`
	FlowType      flow.FlowMessage_FlowType `json:"type"`
	SamplingRate  []byte                    `json:"sampling"`
	Srcipstr      string                    `json:"src_ip"`
	Dstipstr      string                    `json:"dst_ip"`
	Bytes         uint64                    `json:"bytes"`
	Packets       uint64                    `json:"packets"`
	SrcPort       uint32                    `json:"src_port"`
	DstPort       uint32                    `json:"dst_port"`
	Etype         uint32                    `json:"etype"`
	Protocol      uint32                    `json:"proto"`
	SrcAS         uint32                    `json:"src_as"`
	DstAS         uint32                    `json:"dst_as"`
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready  chan bool
	config *AppConfig
}

// Setup is run at the beginning of a new session, before ConsumeClaim
// Mark the consumer as ready
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func strategyForeverProcess(session *sarama.ConsumerGroupSession, claim *sarama.ConsumerGroupClaim) {
	log.Printf("Start processing with member-id: '%v'", (*session).MemberID())

	for message := range (*claim).Messages() {
		var fmsg flow.FlowMessage

		err := proto.Unmarshal(message.Value, &fmsg)
		if err != nil {
			log.Panicf("[ERROR] Flow unmarshaling error: %v\n", err)
		} else {
			timeStart := time.Unix(int64(fmsg.TimeFlowStart), 0)

			srcip := net.IP(fmsg.SrcAddr)
			srcipstr := srcip.String()
			if srcipstr == "<nil>" {
				srcipstr = "0.0.0.0"
			}

			dstip := net.IP(fmsg.DstAddr)
			dstipstr := dstip.String()
			if dstipstr == "<nil>" {
				dstipstr = "0.0.0.0"
			}

			flowData := FlowToExport{
				TimeFlowStart: timeStart,
				FlowType:      fmsg.Type,
				SamplingRate:  fmsg.SamplerAddress,
				Srcipstr:      srcipstr,
				Dstipstr:      dstipstr,
				Bytes:         fmsg.Bytes,
				Packets:       fmsg.Packets,
				SrcPort:       fmsg.SrcPort,
				DstPort:       fmsg.DstPort,
				Etype:         fmsg.Etype,
				Protocol:      fmsg.Proto,
				SrcAS:         fmsg.SrcAS,
				DstAS:         fmsg.DstAS,
			}

			flowDataBytes, err := json.Marshal(flowData)
			flowDataStr := string(flowDataBytes)

			if err != nil {
				log.Printf("[ERROR] Flow marshaling error: %v\n", err)
			} else {
				fmt.Printf("%v\n", flowDataStr)
				log.Printf("Taked by '%v': %v", (*session).MemberID(), flowDataStr)
			}
		}

		(*session).MarkMessage(message, "")

	}
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	switch consumer.config.Strategy {
	case workStrategyForever:
		strategyForeverProcess(&session, &claim)
	default:
		log.Panicf("Unrecognized application work strategy: %s", consumer.config.Strategy)
	}

	return nil
}

func Consume(conf *AppConfig) (err error) {
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		log.Printf("[ERROR] Failed to parse Kafka version: %v\n", err)
		return
	}

	config := sarama.NewConfig()
	config.Version = version

	switch conf.Assignor {
	case strategySticky:
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case strategyRoundrobin:
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case strategyRage:
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", conf.Assignor)
	}

	if conf.Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		log.Println("Consume initial offset from oldest")
	}

	consumer := Consumer{
		ready:  make(chan bool),
		config: conf,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(conf.KafkaBrokers, ","), conf.Group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v\n", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			if err := client.Consume(ctx, strings.Split(conf.Topics, ","), &consumer); err != nil {
				log.Printf("Error from consumer: %v\n", err)
			}

			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("TERMINATING: context cancelled")
	case <-sigterm:
		log.Println("TERMINATING: via signal")
	}

	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Printf("Error closing client: %v\n", err)
	}

	return nil
}

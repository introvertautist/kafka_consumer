package main

import (
	kafka "github.com/introvertautist/kafka_consumer/consumer/kafka"
	cli "github.com/jawher/mow.cli"
	"github.com/natefinch/lumberjack"
	"log"
	"os"
	"reflect"
)

func logInit() {
	log.SetOutput(&lumberjack.Logger{
		Filename:   "/var/log/consumectl.log",
		MaxSize:    500,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
	})
}

func printConfig(config *kafka.AppConfig) {
	log.Println("Run consumer with params:")

	fields := reflect.TypeOf(*config)
	values := reflect.ValueOf(*config)

	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		value := values.Field(i)
		log.Printf("%15s: %v\n", field.Name, value)
	}
}

func parseKafkaArgs(cmd *cli.Cmd) *kafka.AppConfig {
	cmd.Spec = "--group=<group> --topics=<topics> " +
		"[--strategy=<strategy>] " +
		"[--brokers=<brokers>] " +
		"[--version=<version>] " +
		"[--assignor=<assignor>] " +
		"[--oldest=<oldest>] "

	var config kafka.AppConfig

	cmd.StringOptPtr(&config.Group, "group", "", "Kafka consumer group definition")
	cmd.StringOptPtr(&config.Topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")

	cmd.StringOptPtr(&config.Strategy, "strategy", "forever", "Application work strategy")
	cmd.StringOptPtr(&config.KafkaBrokers, "brokers", "127.0.0.1:9092,[::1]:9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	cmd.StringOptPtr(&config.Version, "version", "2.1.1", "Kafka cluster version")
	cmd.StringOptPtr(&config.Assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	cmd.BoolOptPtr(&config.Oldest, "oldest", false, "Kafka consumer consume initial offset from oldest")

	return &config
}

func prepareKafkaConsuming(cmd *cli.Cmd) {
	config := parseKafkaArgs(cmd)

	cmd.Action = func() {
		log.Print("Start kafka consuming...")
		printConfig(config)

		err := kafka.Consume(config)
		if err != nil {
			log.Printf("Got error, while consuming: %v\n", err)
			cli.Exit(1)
		}
	}
}

func main() {
	app := cli.App("consumectl", "Consuming data from kafka")

	app.Command("kafka", "Watch for new messages in kafka", prepareKafkaConsuming)

	logInit()

	err := app.Run(os.Args)
	if err != nil {
		log.Printf("Got fatal error: %v\n", err)
		cli.Exit(1)
	}

}

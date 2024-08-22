package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	exporter, err := autoexport.NewSpanExporter(context.Background())
	if err != nil {
		log.Fatalf("Error creating span exporter: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		),
	)

	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue, err := rsmq.New(rsmq.Options{
		Client:         cc,
		TracerProvider: tp,
		Topic:          "stream_produce_and_consume",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Consume tasks
	err = queue.Consume(
		context.Background(),
		func(ctx context.Context, task *rsmq.Message) error {
			var payload map[string]interface{}
			json.Unmarshal(task.Payload, &payload)

			return nil
		},
	)
	if err != nil {
		log.Fatalf("Error consuming tasks: %v", err)
	}
}

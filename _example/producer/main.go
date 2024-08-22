package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	ctx, span := tp.Tracer("").Start(context.Background(), "root")
	defer span.End()

	queue, err := rsmq.New(rsmq.Options{
		Client:         cc,
		TracerProvider: tp,
		Topic:          "rsmq_example",
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}
	defer queue.Close()

	// Produce tasks
	for i := 0; i < 10; i++ {
		task := &rsmq.Message{
			Id:      fmt.Sprintf("task-%d", i),
			Payload: json.RawMessage(fmt.Sprintf(`{"message": "Hello %d"}`, i)),
		}
		if i%2 == 0 {
			task.DeliverTimestamp = timestamppb.New(time.Now().Add(3 * time.Second))
		}
		err := queue.Add(ctx, task)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
		}
	}
}

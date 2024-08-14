package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestOtel(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{}, propagation.Baggage{},
		),
	)
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := rsmq.New(rsmq.Options{
		Client:         cc,
		Topic:          "otel",
		TracerProvider: tp,
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	defer queue.Close()

	task := &rsmq.Message{
		Payload: json.RawMessage(`{"message": "Hello world"}`),
	}

	ctx, span := tp.Tracer("rsmq").Start(context.Background(), "otel")
	defer span.End()

	err := queue.Add(ctx, task)
	require.Nil(t, err)

	consumeDone := make(chan struct{})

	go func() {
		_ = queue.Consume(context.Background(), func(ctx context.Context, task *rsmq.Message) error {
			fmt.Println(task.String(), task.GetMetadata())
			fmt.Println(trace.SpanContextFromContext(ctx).TraceID().String())

			consumeDone <- struct{}{}

			return nil
		})
	}()

	<-consumeDone
	time.Sleep(time.Second)

	fmt.Printf("%+v %d\n", exporter.GetSpans().Snapshots(), len(exporter.GetSpans().Snapshots()))
	spans := exporter.GetSpans().Snapshots()
	if len(spans) == 2 && spans[0].Name() == "Enqueue" && spans[1].Name() == "ConsumeStream" {
		return
	}

	t.Fail()
}

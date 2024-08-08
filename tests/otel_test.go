package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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
		Client: cc,
		Stream: "otel",
		Tracer: tp.Tracer("rsmq"),
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

	queue.Enqueue(ctx, task)

	consumeDone := make(chan struct{})

	go func() {
		queue.Consume(context.Background(), func(ctx context.Context, task *rsmq.Message) *rsmq.Result {
			fmt.Println(task.String(), task.GetMetadata())
			fmt.Println(trace.SpanContextFromContext(ctx).TraceID().String())

			close(consumeDone)

			return &rsmq.Result{
				Id: task.Id,
			}
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

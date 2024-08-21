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
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

	queue, err := rsmq.New(rsmq.Options{
		Client:         cc,
		Topic:          "otel",
		TracerProvider: tp,
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	require.Nil(t, err)
	defer queue.Close()

	ctx, span := tp.Tracer("rsmq").Start(context.Background(), "otel")
	defer span.End()

	for i := 0; i < 3; i++ {
		task := &rsmq.Message{
			Payload: json.RawMessage(`{"message": "Hello world"}`),
		}

		err = queue.Add(ctx, task)
		require.Nil(t, err)
	}

	go func() {
		_ = queue.Consume(context.Background(), func(ctx context.Context, task *rsmq.Message) error {
			fmt.Println(task.String(), task.GetMetadata())
			fmt.Println(trace.SpanContextFromContext(ctx).TraceID().String())

			return nil
		})
	}()

	time.Sleep(time.Second)

	fmt.Printf("%+v %d\n", exporter.GetSpans().Snapshots(), len(exporter.GetSpans().Snapshots()))
	spans := exporter.GetSpans().Snapshots()
	if len(spans) == 4 && spans[0].Name() == "Add" && spans[1].Name() == "Add" && spans[2].Name() == "Add" && spans[3].Name() == "ConsumeStream" {
		traceId := spans[0].SpanContext().TraceID().String()
		linkTraceId := trace.LinkFromContext(ctx).SpanContext.TraceID().String()
		require.Equal(t, traceId, linkTraceId)
		require.Equal(t, spans[3].Attributes()[0], rsmq.MessagingRsmqSystem)
		require.Equal(t, spans[3].Attributes()[1], rsmq.MessagingRsmqMessageTopic.String("otel"))
		require.Equal(t, spans[3].Attributes()[2], rsmq.MessagingRsmqMessageGroup.String("task_group"))
		require.Equal(t, spans[3].Attributes()[3], semconv.MessagingBatchMessageCount(3))

		return
	}

	t.Fail()
}

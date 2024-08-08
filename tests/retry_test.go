package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
)

func TestRetry(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := rsmq.New(rsmq.Options{
		Client:    cc,
		StreamKey: "retry",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			RetryTimeWait:   10 * time.Millisecond,
		},
	})
	defer queue.Close()

	// Produce tasks

	task := &rsmq.Message{
		Payload: json.RawMessage(`{"message": "Hello world"}`),
	}

	err := queue.Enqueue(context.Background(), task)
	if err != nil {
		log.Printf("Failed to enqueue task: %v", err)
	}

	var count atomic.Int32
	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) *rsmq.Result {
				var payload map[string]interface{}
				json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task: %s, payload: %v\n", task.Id, payload)
				count.Add(1)
				return &rsmq.Result{
					Id:    task.Id,
					Error: "retry test",
				}
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(time.Second)
	require.Equal(t, int32(4), count.Load())
}

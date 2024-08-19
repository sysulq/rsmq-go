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

func TestPending(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queuePending := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "pending",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			ConsumerID:      "pending",
			AutoCreateGroup: true,
			PendingTimeout:  time.Millisecond,
		},
	})
	defer queuePending.Close()
	go func() {
		_ = queuePending.Consume(context.Background(), func(ctx context.Context, m *rsmq.Message) error {
			time.Sleep(10 * time.Second)
			fmt.Println("Pending consumer", m.Id)
			return nil
		})
	}()

	// Produce tasks
	task := &rsmq.Message{
		Payload: json.RawMessage(`{"message": "Hello world"}`),
	}
	for i := 0; i < 4; i++ {
		err := queuePending.Add(context.Background(), task)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
		}
	}

	queue := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "pending",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			ConsumerID:      "normal",
			AutoCreateGroup: true,
			PendingTimeout:  time.Millisecond,
		},
	})
	defer queue.Close()

	var count atomic.Int32
	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				count.Add(1)
				return nil
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)
	require.Equal(t, int32(4), count.Load())
}

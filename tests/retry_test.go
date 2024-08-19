package tests

import (
	"context"
	"encoding/json"
	"errors"
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
		Client: cc,
		Topic:  "retry",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			RetryTimeWait:   10 * time.Millisecond,
		},
	})
	defer queue.Close()
	defer func() {
		_, err := cc.Del(context.Background(), "rsmq:{retry}:dlq").Result()
		require.NoError(t, err)
	}()

	// Produce tasks

	task := &rsmq.Message{
		Payload: json.RawMessage(`{"message": "Hello world"}`),
	}

	err := queue.Add(context.Background(), task)
	if err != nil {
		log.Printf("Failed to enqueue task: %v", err)
	}

	var count atomic.Int32

	results := make(chan *rsmq.Message, 4)
	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task: %s, payload: %v\n", task.Id, payload)
				count.Add(1)
				results <- task

				return errors.New("retry test")
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(time.Second)
	require.Equal(t, int32(4), count.Load())

	payloads := make([]*rsmq.Message, 4)
	for i := 0; i < 4; i++ {
		payloads[i] = <-results
	}

	fmt.Printf("Payloads:\n%+v\n", payloads)

	originMsgId := payloads[0].Id
	require.Empty(t, payloads[0].GetOriginMsgId())
	for i := 1; i < 4; i++ {
		require.Equal(t, originMsgId, payloads[i].GetOriginMsgId())
		require.EqualValues(t, i, payloads[i].GetRetryCount())
	}

	len, err := cc.XLen(context.Background(), "rsmq:{retry}:dlq").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), len)
}

package rsmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProduceAndConsume(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := rsmq.New(rsmq.Options{
		Client: cc,
		Stream: "example",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	defer queue.Close()

	// Produce tasks
	for i := 0; i < 10; i++ {
		task := &rsmq.Message{
			Payload: json.RawMessage(fmt.Sprintf(`{"message": "Hello %d"}`, i)),
		}
		if i%2 == 0 {
			task.DeliverTimestamp = timestamppb.New(time.Now().Add(time.Second))
		}
		err := queue.Add(context.Background(), task)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
		}
	}

	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task: %s, payload: %v\n", task.Id, payload)

				return nil
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(time.Second)
	// Output:
	// Processing task: 1723447963244-0, payload: map[message:Hello 1]
	// Processing task: 1723447963246-1, payload: map[message:Hello 9]
	// Processing task: 1723447963245-1, payload: map[message:Hello 5]
	// Processing task: 1723447963246-0, payload: map[message:Hello 7]
	// Processing task: 1723447963245-0, payload: map[message:Hello 3]
	// Processing task: 1723447964249-1, payload: map[message:Hello 2]
	// Processing task: 1723447964249-0, payload: map[message:Hello 4]
	// Processing task: 1723447964249-4, payload: map[message:Hello 0]
	// Processing task: 1723447964249-2, payload: map[message:Hello 8]
	// Processing task: 1723447964249-3, payload: map[message:Hello 6]
}

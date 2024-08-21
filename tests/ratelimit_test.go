package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRateLimit(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue, err := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "rate_limit",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			MaxConcurrency:  1,
			RateLimit:       3,
		},
	})
	require.Nil(t, err)
	defer queue.Close()

	waitProducer := make(chan struct{})
	// Produce tasks
	go func() {
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

		close(waitProducer)
	}()

	<-waitProducer

	results := make(chan map[string]interface{})
	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task: %s, payload: %v\n", task.Id, payload)

				results <- payload

				return nil
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	now := time.Now()

	resultsList := make([]map[string]interface{}, 0)
	for i := 0; i < 10; i++ {
		result := <-results
		resultsList = append(resultsList, result)
	}

	if time.Since(now) < 2*time.Second {
		t.Errorf("Expected to take at least 2 seconds, took %s", time.Since(now))
	}

	numbers := make([]int, 0)
	for _, result := range resultsList {
		if !strings.HasPrefix(result["message"].(string), "Hello ") {
			t.Errorf("Expected result ID to start with 'Hello ', got %s", result)
		}

		number := result["message"].(string)[6:]
		n, err := strconv.Atoi(number)
		require.Nil(t, err)
		numbers = append(numbers, n)
	}

	slices.Sort(numbers)
	for i := 0; i < 10; i++ {
		if numbers[i] != i {
			t.Errorf("Expected number to be %d, got %d", i, numbers[i])
		}
	}
}

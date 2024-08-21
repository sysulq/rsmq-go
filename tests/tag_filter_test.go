package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
)

func TestTagFilter(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue, err := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "tag_filter",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			SubExpression:   "tagA||tagB",
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
			if i%2 == 1 {
				task.Tag = "tagA"
			}
			err := queue.Add(context.Background(), task)
			if err != nil {
				log.Printf("Failed to enqueue task: %v", err)
			}
		}

		close(waitProducer)
	}()

	<-waitProducer

	var counts uint32
	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task: %s, payload: %v\n", task.Id, payload)

				if !strings.HasPrefix(payload["message"].(string), "Hello ") {
					t.Errorf("Expected result ID to start with 'Hello ', got %s", payload)
				}

				number := payload["message"].(string)[6:]
				if number != "1" && number != "3" && number != "5" && number != "7" && number != "9" {
					t.Errorf("Expected odd number, got %s", number)
				}

				n, err := strconv.Atoi(number)
				require.Nil(t, err)

				atomic.AddUint32(&counts, uint32(n))

				return nil
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(time.Second)
	if atomic.LoadUint32(&counts) != 25 {
		t.Errorf("Expected sum of odd numbers to be 25, got %d", atomic.LoadUint32(&counts))
	}
}

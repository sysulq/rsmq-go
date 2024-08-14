package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
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
		Topic:  "stream_produce_and_consume",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
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

	resultsList := make([]map[string]interface{}, 0)
	for i := 0; i < 10; i++ {
		result := <-results
		resultsList = append(resultsList, result)
	}

	for idx, result := range resultsList {
		if !strings.HasPrefix(result["message"].(string), "Hello ") {
			t.Errorf("Expected result ID to start with 'Hello ', got %s", result)
		}

		number := result["message"].(string)[6:]
		if idx < 5 {
			if number != "1" && number != "3" && number != "5" && number != "7" && number != "9" {
				t.Errorf("Expected odd number, got %s", number)
			}
		} else {
			if number != "0" && number != "2" && number != "4" && number != "6" && number != "8" {
				t.Errorf("Expected even number, got %s", number)
			}
		}
	}
}

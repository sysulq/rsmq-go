package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

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

	// Consume tasks
	err := queue.Consume(
		context.Background(),
		func(ctx context.Context, task *rsmq.Message) error {
			var payload map[string]interface{}
			json.Unmarshal(task.Payload, &payload)

			return nil
		},
	)
	if err != nil {
		log.Fatalf("Error consuming tasks: %v", err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	// Produce tasks
	for i := 0; i < 10; i++ {
		task := &rsmq.Message{
			Id:      fmt.Sprintf("task-%d", i),
			Payload: json.RawMessage(fmt.Sprintf(`{"message": "Hello %d"}`, i)),
		}
		if i%2 == 0 {
			task.DeliverTimestamp = timestamppb.New(time.Now().Add(3 * time.Second))
		}
		err := queue.Enqueue(context.Background(), task)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
		}
	}
}

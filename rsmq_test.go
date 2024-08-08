package rsmq

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestCleanIdleConsumer(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := New(Options{
		Client:    cc,
		StreamKey: "stream_produce_and_consume",
		ConsumeOpts: ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})

	queue.cleanIdleConsumers(context.Background())
}

package rsmq

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCleanIdleConsumer(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := New(Options{
		Client: cc,
		Topic:  "clean_idle_consumer",
		ConsumeOpts: ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})

	go func() { _ = queue.Consume(context.Background(), nil) }()

	time.Sleep(time.Second)

	_, err := queue.cleanIdleConsumers(context.Background())
	require.Nil(t, err)
}

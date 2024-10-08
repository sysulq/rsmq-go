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

	queue, err := New(Options{
		Client: cc,
		Topic:  "clean_idle_consumer",
		ConsumeOpts: ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	require.Nil(t, err)

	go func() { _ = queue.Consume(context.Background(), nil) }()

	time.Sleep(time.Second)

	err = queue.cleanIdleConsumers(context.Background())
	require.Nil(t, err)
}

package rsmq

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCleanIdleConsumer(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := New(Options{
		Client: cc,
		Stream: "stream_produce_and_consume",
		ConsumeOpts: ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})

	_, err := queue.cleanIdleConsumers(context.Background())
	require.Nil(t, err)
}

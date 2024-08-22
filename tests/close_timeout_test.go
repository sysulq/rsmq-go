package tests

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
)

func TestCloseTimeout(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue, err := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "close_timeout",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			CloseTimeout:    100 * time.Millisecond,
		},
	})
	require.Nil(t, err)

	err = queue.Add(context.Background(), &rsmq.Message{
		Payload: []byte(`{"message": "Hello world"}`),
	})
	require.Nil(t, err)

	go func() {
		_ = queue.Consume(context.Background(), func(ctx context.Context, m *rsmq.Message) error {
			time.Sleep(10 * time.Second)
			return nil
		})
	}()

	now := time.Now()
	err = queue.Close()
	require.Nil(t, err)
	require.True(t, time.Since(now) > 100*time.Millisecond)
	require.True(t, time.Since(now) < 1*time.Second)
}

package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/sysulq/rsmq-go"
)

func TestRetentionTime(t *testing.T) {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue, err := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "retention_time",
		RetentionOpts: rsmq.RetentionOpts{
			MaxRetentionTime:       100 * time.Millisecond,
			CheckRetentionInterval: 600 * time.Millisecond,
		},
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
		},
	})
	require.Nil(t, err)
	defer queue.Close()

	// Produce tasks

	task := &rsmq.Message{
		Payload: json.RawMessage(`{"message": "Hello world"}`),
	}

	err = queue.Add(context.Background(), task)
	require.NoError(t, err)

	len, err := cc.XLen(context.Background(), "rsmq:{retention_time}").Result()
	require.NoError(t, err)
	require.Equal(t, len > 0, true, len)

	time.Sleep(time.Second)

	len, err = cc.XLen(context.Background(), "rsmq:{retention_time}").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), len)
}

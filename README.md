rsmq-go
===

[![Go](https://github.com/sysulq/rsmq-go/actions/workflows/go.yml/badge.svg)](https://github.com/sysulq/rsmq-go/actions/workflows/go.yml)

Go implementation of the Message Queue based on Redis Streams.

Features
---

- Add message to the queue
- Consume message from the queue
- Auto-acknowledgment of message
- Message delivery delay with specific timestamp
- Message retry ability
- Dead letter queue after retry limit
- Auto clean idle consumer
- Pending message processing
- Distributed rate limiting
- Tag filter for message
- OpenTelemetry instrumentation

Installation
---

```bash
go get github.com/sysulq/rsmq-go
```

Example
---

```go
package rsmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sysulq/rsmq-go"
)

func Example_produceAndConsume() {
	cc := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	queue := rsmq.New(rsmq.Options{
		Client: cc,
		Topic:  "example",
		ConsumeOpts: rsmq.ConsumeOpts{
			ConsumerGroup:   "task_group",
			AutoCreateGroup: true,
			MaxConcurrency:  1,
		},
	})
	defer queue.Close()

	// Produce tasks
	for i := 0; i < 10; i++ {
		task := &rsmq.Message{
			Payload: json.RawMessage(fmt.Sprintf(`{"message": "Hello %d"}`, i)),
		}

		err := queue.Add(context.Background(), task)
		if err != nil {
			log.Printf("Failed to enqueue task: %v", err)
		}
	}

	// Consume tasks
	go func() {
		err := queue.Consume(
			context.Background(),
			func(ctx context.Context, task *rsmq.Message) error {
				var payload map[string]interface{}
				_ = json.Unmarshal(task.Payload, &payload)
				fmt.Printf("Processing task, payload: %v\n", payload)

				return nil
			},
		)
		if err != nil {
			log.Fatalf("Error consuming tasks: %v", err)
		}
	}()

	time.Sleep(time.Second)
	// Output:
	// Processing task, payload: map[message:Hello 0]
	// Processing task, payload: map[message:Hello 1]
	// Processing task, payload: map[message:Hello 2]
	// Processing task, payload: map[message:Hello 3]
	// Processing task, payload: map[message:Hello 4]
	// Processing task, payload: map[message:Hello 5]
	// Processing task, payload: map[message:Hello 6]
	// Processing task, payload: map[message:Hello 7]
	// Processing task, payload: map[message:Hello 8]
	// Processing task, payload: map[message:Hello 9]
}
```

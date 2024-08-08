package rsmq

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	rsmqv1 "github.com/sysulq/rsmq-go/rsmq/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Message represents a message in the queue
type Message = rsmqv1.Message

// MessageHandler is a function that processes a message and returns a result
type MessageHandler func(context.Context, *Message) error

// MessageQueue manages message production and consumption
type MessageQueue struct {
	opts          Options
	closed        *atomic.Uint32
	processScript *redis.Script
	cron          *cron.Cron
	redisLock     *redislock.Client
	once          sync.Once
}

type Options struct {
	// Client is the Redis client
	Client *redis.Client
	// Stream is the key of the stream
	Stream string
	// MaxLen is the maximum length of the stream
	MaxLen int64
	// Trace is the OpenTelemetry tracer
	Tracer trace.Tracer
	// ConsumeOpts represents options for consuming messages
	ConsumeOpts ConsumeOpts
}

// ConsumeOpts represents options for consuming messages
type ConsumeOpts struct {
	// ConsumerGroup is the name of the consumer group
	ConsumerGroup string
	// ConsumerID is the unique identifier for the consumer
	ConsumerID string
	// BatchSize is the number of messages to consume in a single batch
	BatchSize int64
	// MaxPollInterval is the maximum time to wait between polls
	MaxPollInterval time.Duration
	// MinPollInterval is the minimum time to wait between polls
	MinPollInterval time.Duration
	// BlockDuration is the maximum time to block while waiting for messages
	BlockDuration time.Duration
	// AutoCreateGroup determines whether the consumer group should be created automatically
	AutoCreateGroup bool
	// MaxConcurrency is the maximum number of messages to process concurrently
	MaxConcurrency uint32
	// ConsumerIdleTimeout is the maximum time a consumer can be idle before being removed
	ConsumerIdleTimeout time.Duration
	// MaxRetryLimit is the maximum number of times a message can be retried
	MaxRetryLimit uint32
	// RetryTimeWait is the time to wait before retrying a message
	RetryTimeWait time.Duration
	// PendingTimeout is the time to wait before a pending message is re-queued
	PendingTimeout time.Duration
	// IdleConsumerCleanInterval is the interval to clean idle consumers
	IdleConsumerCleanInterval time.Duration
}

// New creates a new MessageQueue instance
func New(opts Options) *MessageQueue {
	if opts.Client == nil {
		panic("redis client is required")
	}

	if opts.Stream == "" {
		panic("stream key is required")
	}

	if opts.MaxLen == 0 {
		opts.MaxLen = 1000
	}

	if opts.ConsumeOpts.BatchSize == 0 {
		opts.ConsumeOpts.BatchSize = 100
	}
	if opts.ConsumeOpts.MaxPollInterval == 0 {
		opts.ConsumeOpts.MaxPollInterval = time.Second
	}
	if opts.ConsumeOpts.MinPollInterval == 0 {
		opts.ConsumeOpts.MinPollInterval = time.Millisecond * 10
	}
	if opts.ConsumeOpts.BlockDuration == 0 {
		opts.ConsumeOpts.BlockDuration = time.Millisecond * 100 // Default block duration
	}
	if opts.ConsumeOpts.MaxConcurrency == 0 {
		opts.ConsumeOpts.MaxConcurrency = 100
	}
	if opts.ConsumeOpts.ConsumerID == "" {
		opts.ConsumeOpts.ConsumerID = generateConsumerID()
	}
	if opts.ConsumeOpts.ConsumerIdleTimeout == 0 {
		opts.ConsumeOpts.ConsumerIdleTimeout = 2 * time.Hour
	}
	if opts.ConsumeOpts.MaxRetryLimit == 0 {
		opts.ConsumeOpts.MaxRetryLimit = 3
	}
	if opts.ConsumeOpts.RetryTimeWait == 0 {
		opts.ConsumeOpts.RetryTimeWait = 5 * time.Second
	}
	if opts.ConsumeOpts.PendingTimeout == 0 {
		opts.ConsumeOpts.PendingTimeout = time.Minute
	}
	if opts.ConsumeOpts.IdleConsumerCleanInterval == 0 {
		opts.ConsumeOpts.IdleConsumerCleanInterval = 5 * time.Minute
	}

	processScript := redis.NewScript(`
        local delayedSetKey = KEYS[1]
        local streamKey = KEYS[2]
        local now = tonumber(ARGV[1])

        -- Get messages that are ready to be processed
        local messages = redis.call('ZRANGEBYSCORE', delayedSetKey, 0, now)

		local processed = 0
        if #messages > 0 then
            for i = 1, #messages do
                local messageData = messages[i]
                local score = messages[i+1]
                
                -- Add message to the stream
                redis.call('XADD', streamKey, '*', 'message', messageData)
                
                -- Remove the processed message from the delayed set
                redis.call('ZREM', delayedSetKey, messageData)
                
                processed = processed + 1
            end
        end

        return processed
    `)

	mq := &MessageQueue{
		opts:          opts,
		processScript: processScript,
		closed:        &atomic.Uint32{},
		cron:          cron.New(),
		redisLock:     redislock.New(opts.Client),
	}

	return mq
}

func (mq *MessageQueue) enqueueMessage(ctx context.Context, pipe redis.Cmdable, msg *Message) error {
	messageBytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	if msg.GetDeliverTimestamp().GetSeconds() > msg.GetBornTimestamp().GetSeconds() {
		// Add to delayed set with the entire message as the member
		err = pipe.ZAdd(ctx, mq.streamDelayKeyString(), redis.Z{
			Score:  float64(msg.GetDeliverTimestamp().GetSeconds()),
			Member: messageBytes,
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to add message to delayed set: %w", err)
		}

		slog.Debug("added message to delayed set", "id", msg.Id, "deliverTimestamp", msg.DeliverTimestamp)
	} else {
		// Add to stream
		err = pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: mq.streamString(),
			MaxLen: mq.opts.MaxLen,
			Approx: true,
			Values: map[string]interface{}{"message": string(messageBytes)},
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to add message to stream: %w", err)
		}

		slog.Debug("added message to stream", "id", msg.Id)
	}

	return nil
}

// Enqueue adds a new message to the queue
func (mq *MessageQueue) Enqueue(ctx context.Context, message *Message) error {
	if message.GetId() == "" {
		message.Id = uuid.New().String()
	}
	if message.GetBornTimestamp().GetSeconds() == 0 {
		message.BornTimestamp = timestamppb.New(time.Now())
	}
	if message.GetDeliverTimestamp().GetSeconds() == 0 {
		message.DeliverTimestamp = message.GetBornTimestamp()
	}

	message.StreamKey = mq.streamString()

	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		ctx, span = mq.opts.Tracer.Start(ctx, "Enqueue", trace.WithAttributes(
			attribute.String("message.id", message.Id),
			attribute.String("stream", mq.opts.Stream),
		))
		defer span.End()
	}

	message.Metadata = make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(message.Metadata))

	return mq.enqueueMessage(ctx, mq.opts.Client, message)
}

func (mq *MessageQueue) streamDelayKeyString() string {
	return fmt.Sprintf("%s:delayed", mq.streamString())
}

func (mq *MessageQueue) streamString() string {
	return fmt.Sprintf("rsmq:{%s}", mq.opts.Stream)
}

func (mq *MessageQueue) ensureConsumerGroup(ctx context.Context, group string) error {
	// First, ensure the stream exists
	err := mq.opts.Client.XGroupCreateMkStream(ctx, mq.streamString(), group, "$").Err()
	if err != nil {
		// If the error is not because the group already exists, return the error
		if err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return fmt.Errorf("failed to create stream and consumer group: %w", err)
		}
		// If the group already exists, we're done
		return nil
	}

	// If we reach here, it means the stream and group were created successfully
	return nil
}

// Consume starts consuming messages from the queue
func (mq *MessageQueue) Consume(ctx context.Context, handler MessageHandler) error {
	opts := mq.opts.ConsumeOpts

	if opts.ConsumerGroup == "" {
		return fmt.Errorf("consumer group is required")
	}

	mq.once.Do(func() {
		if opts.AutoCreateGroup {
			err := mq.ensureConsumerGroup(context.Background(), opts.ConsumerGroup)
			if err != nil {
				slog.Error("failed to ensure consumer group", "error", err)
				return
			}
		}

		mq.cron.Schedule(cron.Every(opts.IdleConsumerCleanInterval), cron.FuncJob(func() {
			_, _ = mq.cleanIdleConsumers(context.Background())
		}))

		mq.cron.Start()
	})

	currentPollInterval := opts.MinPollInterval

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if mq.closed.Load() == 1 {
				return nil
			}

			start := time.Now()

			// Process delayed messages
			delayedProcessed, err := mq.processDelayedMessages(ctx)
			if err != nil {
				slog.Error("failed to process delayed messages", "error", err)
			}

			// Consume from stream
			streamProcessed, err := mq.consumeStream(ctx, handler)
			if err != nil {
				slog.Error("failed to consume from stream", "error", err)
			}

			// Adjust polling interval based on activity
			if delayedProcessed > 0 || streamProcessed > 0 {
				currentPollInterval = opts.MinPollInterval
			} else {
				currentPollInterval = min(currentPollInterval*2, opts.MaxPollInterval)
			}

			// Wait for the next poll, but don't exceed the time until the next delayed message
			nextDelayedTime, err := mq.getNextDelayedMessageTime(ctx)
			if err != nil {
				slog.Error("failed to get next delayed message time", "error", err)
			} else if !nextDelayedTime.IsZero() {
				currentPollInterval = min(currentPollInterval, time.Until(nextDelayedTime))
			}

			// Determine the appropriate block duration
			blockDuration := opts.BlockDuration
			if !nextDelayedTime.IsZero() {
				timeUntilNext := time.Until(nextDelayedTime)
				if timeUntilNext < blockDuration {
					blockDuration = timeUntilNext
				}
			}

			// If no messages were processed and we didn't block, wait for a short time
			if delayedProcessed == 0 && streamProcessed == 0 && time.Since(start) < blockDuration {
				time.Sleep(opts.MinPollInterval)
			}
		}
	}
}

func (mq *MessageQueue) Close() error {
	if !mq.closed.CompareAndSwap(0, 1) {
		return nil
	}

	mq.cron.Stop()

	err := mq.opts.Client.XGroupDelConsumer(
		context.Background(), mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, mq.opts.ConsumeOpts.ConsumerID).Err()
	if err != nil {
		slog.Error("failed to remove consumer from group", "error", err)
	}

	return nil
}

func (mq *MessageQueue) processDelayedMessages(ctx context.Context) (int, error) {
	now := time.Now().Unix()

	result, err := mq.processScript.Run(ctx, mq.opts.Client, []string{mq.streamDelayKeyString(), mq.streamString()}, now).Int()
	if err != nil {
		return 0, fmt.Errorf("failed to process delayed messages: %w", err)
	}

	return result, nil
}

func (mq *MessageQueue) consumeStream(ctx context.Context, handler MessageHandler) (uint32, error) {
	var processed uint32

	// 首先处理正常的消息流
	normalProcessed, err := mq.processNormalMessages(ctx, handler)
	if err != nil {
		return processed, err
	}
	processed += normalProcessed

	// 如果没有新的正常消息，尝试处理pending消息
	if normalProcessed == 0 {
		pendingProcessed, err := mq.processPendingMessages(ctx, handler)
		if err != nil {
			slog.Error("failed to process pending messages", "error", err)
		}
		processed += pendingProcessed
	}

	return processed, nil
}

func (mq *MessageQueue) processNormalMessages(ctx context.Context, handler MessageHandler) (uint32, error) {
	streams, err := mq.opts.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    mq.opts.ConsumeOpts.ConsumerGroup,
		Consumer: mq.opts.ConsumeOpts.ConsumerID,
		Streams:  []string{mq.streamString(), ">"},
		Count:    mq.opts.ConsumeOpts.BatchSize,
		Block:    mq.opts.ConsumeOpts.BlockDuration,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil // 没有消息
		}
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			if err := mq.ensureConsumerGroup(ctx, mq.opts.ConsumeOpts.ConsumerGroup); err != nil {
				return 0, err
			}

			return mq.processNormalMessages(ctx, handler)
		}
		return 0, fmt.Errorf("failed to read from stream: %w", err)
	}

	var wg sync.WaitGroup
	var processed uint32
	errors := make(chan error, mq.opts.ConsumeOpts.BatchSize)
	semaphore := make(chan struct{}, mq.opts.ConsumeOpts.MaxConcurrency)

	for _, stream := range streams {
		slog.Debug("read messages", "stream", stream.Stream, "count", len(stream.Messages), "consumer", mq.opts.ConsumeOpts.ConsumerID)

		for _, message := range stream.Messages {

			wg.Add(1)
			semaphore <- struct{}{} // 获取信号量

			go func(msg redis.XMessage) {
				defer wg.Done()
				defer func() { <-semaphore }() // 释放信号量

				m, err := mq.unmarshalMessage(msg)
				if err != nil {
					errors <- fmt.Errorf("failed to unmarshal message: %w", err)
					return
				}

				slog.Debug("processing message", "msg", m.String())

				ctx := context.Background()
				if mq.opts.Tracer != nil {
					ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.GetMetadata()))
					var span trace.Span
					ctx, span = mq.opts.Tracer.Start(ctx, "ConsumeStream", trace.WithAttributes(
						attribute.String("stream", mq.opts.Stream),
						attribute.String("consumer_group", mq.opts.ConsumeOpts.ConsumerGroup),
					))
					defer span.End()
				}

				result := handler(ctx, m)

				if result == nil {
					// Message processed successfully
					if err := mq.opts.Client.XAck(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, msg.ID).Err(); err != nil {
						errors <- fmt.Errorf("failed to acknowledge message %s: %w", msg.ID, err)
						return
					}
					atomic.AddUint32(&processed, 1)
				} else {
					// Message processing failed, implement retry logic
					if atomic.AddUint32(&m.RetryCount, 1) <= mq.opts.ConsumeOpts.MaxRetryLimit {
						// Re-enqueue the message with updated retry count
						if err := mq.retry(ctx, m); err != nil {
							errors <- fmt.Errorf("failed to re-enqueue message %s: %w", m.Id, err)
							return
						}
						slog.Info("message requeued for retry", "id", m.Id, "retry_count", m.RetryCount)
					} else {
						// Max retries reached, handle accordingly (e.g., move to dead letter queue)
						slog.Warn("message reached max retry limit", "id", m.Id, "error", result.Error)
						// Here you might want to implement dead letter queue logic
					}
					// Acknowledge the message to remove it from the pending list
					if err := mq.opts.Client.XAck(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, msg.ID).Err(); err != nil {
						errors <- fmt.Errorf("failed to acknowledge failed message %s: %w", msg.ID, err)
						return
					}
				}
			}(message)
		}
	}

	wg.Wait()
	close(errors)

	// 收集并报告错误
	var errs []error
	for err := range errors {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return processed, fmt.Errorf("encountered %d errors during message processing: %v", len(errs), errs)
	}

	return processed, nil
}

func (mq *MessageQueue) getNextDelayedMessageTime(ctx context.Context) (time.Time, error) {
	result, err := mq.opts.Client.ZRangeWithScores(ctx, mq.streamDelayKeyString(), 0, 0).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get next delayed message time: %w", err)
	}

	if len(result) == 0 {
		return time.Time{}, nil
	}

	return time.Unix(int64(result[0].Score), 0), nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func generateConsumerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%s", hostname, uuid.New().String())
}

func (mq *MessageQueue) cleanIdleConsumers(ctx context.Context) (int, error) {
	lock, err := mq.redisLock.Obtain(ctx, "rsmq:lock:"+mq.opts.Stream, 3*time.Second, &redislock.Options{})
	if err != nil && err != redislock.ErrNotObtained {
		slog.Error("failed to obtain lock", "error", err)
		return 0, err
	}
	defer func() { _ = lock.Release(ctx) }()

	consumers, err := mq.opts.Client.XInfoConsumers(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup).Result()
	if err != nil {
		return 0, err
	}

	for _, consumer := range consumers {
		// skip the current stream consumer
		if consumer.Name == mq.opts.ConsumeOpts.ConsumerID {
			continue
		}

		if consumer.Idle > mq.opts.ConsumeOpts.ConsumerIdleTimeout {
			_ = mq.opts.Client.XGroupDelConsumer(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, consumer.Name).Err()
		}
	}
	return 0, nil
}

func (q *MessageQueue) retry(ctx context.Context, msg *Message) error {
	// Make the delete and re-queue operation atomic in case we crash midway
	// and lose a message.
	pipe := q.opts.Client.TxPipeline()
	// When retry a msg, ack it before we delete msg.
	if err := pipe.XAck(ctx, q.streamString(), q.opts.ConsumeOpts.ConsumerGroup, msg.GetId()).Err(); err != nil {
		return err
	}

	err := pipe.XDel(ctx, q.streamString(), msg.GetId()).Err()
	if err != nil {
		return err
	}

	msg.DeliverTimestamp = timestamppb.New(
		time.Now().Add(time.Duration(math.Pow(2, float64(msg.RetryCount))) * q.opts.ConsumeOpts.RetryTimeWait),
	)
	err = q.enqueueMessage(ctx, pipe, msg)
	if err != nil {
		return err
	}

	_, err = pipe.Exec(ctx)
	return err
}

func (mq *MessageQueue) unmarshalMessage(msg redis.XMessage) (*Message, error) {
	messageJSON := msg.Values["message"].(string)
	var m Message
	if err := proto.Unmarshal([]byte(messageJSON), &m); err != nil {
		return nil, fmt.Errorf("error unmarshaling message: %w", err)
	}
	m.Id = msg.ID
	return &m, nil
}

func (mq *MessageQueue) processPendingMessages(ctx context.Context, handler MessageHandler) (uint32, error) {
	claimed, _, err := mq.opts.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   mq.streamString(),
		Group:    mq.opts.ConsumeOpts.ConsumerGroup,
		Consumer: mq.opts.ConsumeOpts.ConsumerID,
		Start:    "-",
		MinIdle:  mq.opts.ConsumeOpts.PendingTimeout,
		Count:    mq.opts.ConsumeOpts.BatchSize,
	}).Result()
	if err != nil {
		slog.Error("failed to claim pending message", "error", err)
		return 0, err
	}

	var processed uint32
	for _, msg := range claimed {
		m, err := mq.unmarshalMessage(msg)
		if err != nil {
			slog.Error("failed to unmarshal claimed message", "message_id", msg.ID, "error", err)
			continue
		}

		result := handler(ctx, m)
		if result == nil {
			if err := mq.opts.Client.XAck(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, msg.ID).Err(); err != nil {
				slog.Error("failed to acknowledge claimed message", "message_id", msg.ID, "error", err)
			} else {
				atomic.AddUint32(&processed, 1)
			}
		} else {
			slog.Error("failed to process claimed message", "message_id", msg.ID, "error", result.Error)
			// 可以在这里实现重试逻辑
		}
	}

	return processed, nil
}

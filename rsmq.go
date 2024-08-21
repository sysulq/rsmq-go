package rsmq

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"dario.cat/mergo"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis_rate/v10"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	rsmqv1 "github.com/sysulq/rsmq-go/rsmq/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Message represents a message in the queue
type Message = rsmqv1.Message

// MessageHandler is a function that processes a message and returns a result
type MessageHandler func(context.Context, *Message) error

// BatchMessageHandler is a function that processes a batch of messages and returns a list of errors
type BatchMessageHandler func(context.Context, []*Message) []error

// MessageQueue manages message production and consumption
type MessageQueue struct {
	opts        Options
	closed      *atomic.Uint32
	delayScript *redis.Script
	cron        *cron.Cron
	redisLock   *redislock.Client
	rateLimit   *redis_rate.Limiter

	subExpressionMap map[string]struct{}
}

type Options struct {
	// Client is the Redis client
	// Must be set
	Client redis.Cmdable
	// Topic is the topic name of the message
	// Must be set
	Topic string
	// RetentionOpts represents options for retention policy
	RetentionOpts RetentionOpts
	// TracerProvider is the OpenTelemetry tracer provider
	TracerProvider trace.TracerProvider
	// ConsumeOpts represents options for consuming messages
	ConsumeOpts ConsumeOpts
}

// RetentionOpts represents options for retention policy
type RetentionOpts struct {
	// MaxLen is the maximum length of the stream
	// Default is 20,000,000
	MaxLen int64
	// MaxRetentionTime is the maximum retention time of the stream
	// Default is 168 hours
	MaxRetentionTime time.Duration
	// CheckRetentionInterval is the interval to check retention time
	// Default is 5 minutes
	CheckRetentionInterval time.Duration
}

// ConsumeOpts represents options for consuming messages
type ConsumeOpts struct {
	// ConsumerGroup is the name of the consumer group
	// Must be set if consuming messages
	ConsumerGroup string
	// ConsumerID is the unique identifier for the consumer
	// Default is generated based on hostname and process ID
	ConsumerID string
	// BatchSize is the number of messages to consume in a single batch
	// If set, the consumer will consume messages in batches
	BatchSize int64
	// MaxBlockDuration is the maximum time to block while waiting for messages
	// If set, the consumer will block for the specified duration
	MaxBlockDuration time.Duration
	// AutoCreateGroup determines whether the consumer group should be created automatically
	// If set, the consumer group will be created if it does not exist
	AutoCreateGroup bool
	// MaxConcurrency is the maximum number of messages to process concurrently
	// If set, the messages will be processed concurrently up to the limit
	MaxConcurrency uint32
	// ConsumerIdleTimeout is the maximum time a consumer can be idle before being removed
	// If set, the idle consumers will be removed periodically
	ConsumerIdleTimeout time.Duration
	// MaxRetryLimit is the maximum number of times a message can be retried
	// If set, the message will be re-queued with an exponential backoff
	MaxRetryLimit uint32
	// RetryTimeWait is the time to wait before retrying a message
	// The time to wait is calculated as 2^retryCount * RetryTimeWait
	RetryTimeWait time.Duration
	// PendingTimeout is the time to wait before a pending message is re-queued
	// If set, the pending messages will be re-queued after the timeout
	PendingTimeout time.Duration
	// IdleConsumerCleanInterval is the interval to clean idle consumers
	// If set, the idle consumers will be removed periodically
	IdleConsumerCleanInterval time.Duration
	// RateLimit is the maximum number of messages to consume per second
	// If set, the rate limiter will be used to limit the number of messages consumed
	RateLimit int
	// SubExpression is the sub expression to filter messages, default is "*"
	// e.g. "tag1||tag2||tag3"
	SubExpression string
}

//go:embed delay.lua
var delayScript string

var (
	// MessagingRsmqSystem is the messaging system for rsmq
	MessagingRsmqSystem = attribute.Key("messaging.system").String("rsmq")
	// MessagingRsmqMessageTopic is the messaging topic for rsmq
	MessagingRsmqMessageTopic = attribute.Key("messaging.rsmq.message.topic")
	// MessagingRsmqMessageGroup is the messaging group for rsmq
	MessagingRsmqMessageGroup = attribute.Key("messaging.rsmq.message.group")
	// MessagingRsmqMessageID is the messaging ID for rsmq
	MessagingRsmqMessageTag = attribute.Key("messaging.rsmq.message.tag")
	// MessagingRsmqMessageID is the messaging ID for rsmq
	MessagingRsmqMessageDeliveryTimestamp = attribute.Key("messaging.rsmq.message.delivery_timestamp")
)

// New creates a new MessageQueue instance
func New(opts Options) (*MessageQueue, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("client is required")
	}

	if opts.Topic == "" {
		return nil, fmt.Errorf("topic is required")
	}

	defaultOpts := Options{
		RetentionOpts: RetentionOpts{
			MaxLen:                 20000000,
			MaxRetentionTime:       168 * time.Hour,
			CheckRetentionInterval: 5 * time.Minute,
		},
		ConsumeOpts: ConsumeOpts{
			BatchSize:                 100,
			MaxBlockDuration:          100 * time.Millisecond,
			MaxConcurrency:            100,
			ConsumerID:                generateConsumerID(),
			ConsumerIdleTimeout:       2 * time.Hour,
			MaxRetryLimit:             3,
			RetryTimeWait:             5 * time.Second,
			PendingTimeout:            time.Minute,
			IdleConsumerCleanInterval: 5 * time.Minute,
			SubExpression:             "*",
		},
	}

	if err := mergo.Merge(&opts, defaultOpts); err != nil {
		return nil, fmt.Errorf("failed to merge options: %w", err)
	}

	mq := &MessageQueue{
		opts:        opts,
		delayScript: redis.NewScript(delayScript),
		closed:      &atomic.Uint32{},
		cron:        cron.New(),
		redisLock:   redislock.New(opts.Client),
	}

	// Ensure the stream group specified in the options
	if opts.ConsumeOpts.ConsumerGroup != "" {

		if opts.ConsumeOpts.SubExpression != "*" {
			mq.subExpressionMap = maps.Collect(Map2(func(k int, v string) (string, struct{}) {
				return v, struct{}{}
			}, slices.All(strings.Split(opts.ConsumeOpts.SubExpression, "||"))))
		}

		if opts.ConsumeOpts.AutoCreateGroup {
			err := mq.ensureConsumerGroup(context.Background(), opts.ConsumeOpts.ConsumerGroup)
			if err != nil {
				return nil, fmt.Errorf("failed to ensure consumer group: %w", err)
			}
		}

		if opts.ConsumeOpts.RateLimit > 0 {
			mq.rateLimit = redis_rate.NewLimiter(opts.Client)
		}

		mq.cron.Schedule(cron.Every(opts.ConsumeOpts.IdleConsumerCleanInterval), cron.FuncJob(func() {
			err := mq.withRedisLock(context.Background(), mq.streamLockKeyString(), mq.cleanIdleConsumers)
			if err != nil {
				slog.Error("failed to clean idle consumers", "error", err)
			}
		}))

		mq.cron.Schedule(cron.Every(opts.RetentionOpts.CheckRetentionInterval), cron.FuncJob(func() {
			err := mq.withRedisLock(context.Background(), mq.streamLockKeyString(), mq.cleanIdleMessages)
			if err != nil {
				slog.Error("failed to clean idle messages", "error", err)
			}
		}))

		mq.cron.Start()
	}

	return mq, nil
}

// enqueueMessage enqueues a message to the stream or delayed set
func (mq *MessageQueue) enqueueMessage(ctx context.Context, pipe redis.Cmdable, msg *Message) error {
	messageBytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	var span trace.Span
	if span = trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		ctx, span = mq.opts.TracerProvider.Tracer("rsmq").Start(ctx, "Add", trace.WithAttributes(
			MessagingRsmqSystem,
			MessagingRsmqMessageTopic.String(mq.opts.Topic),
			MessagingRsmqMessageTag.String(msg.Tag),
			semconv.MessagingMessageBodySize(len(msg.Payload)),
		))
		defer span.End()

		if msg.Metadata == nil {
			msg.Metadata = make(map[string]string)
		}
		otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(msg.Metadata))
	}

	if msg.GetDeliverTimestamp().GetSeconds() > msg.GetBornTimestamp().GetSeconds() {
		if span != nil && span.SpanContext().IsValid() {
			span.SetAttributes(
				MessagingRsmqMessageDeliveryTimestamp.Int64(msg.DeliverTimestamp.GetSeconds()),
			)
		}

		// Add to delayed set with the entire message as the member
		err = pipe.ZAdd(ctx, mq.streamDelayKeyString(), redis.Z{
			Score:  float64(msg.GetDeliverTimestamp().GetSeconds()),
			Member: messageBytes,
		}).Err()
		if err != nil {
			return fmt.Errorf("failed to add message to delayed set: %w", err)
		}

		slog.DebugContext(ctx, "added message to delayed set", "id", msg.Id, "deliverTimestamp", msg.DeliverTimestamp, "payload", msg.GetPayload())
	} else {
		// Add to stream
		result, err := pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: mq.streamString(),
			MaxLen: mq.opts.RetentionOpts.MaxLen,
			Approx: true,
			Values: map[string]interface{}{"message": string(messageBytes)},
		}).Result()
		if err != nil {
			return fmt.Errorf("failed to add message to stream: %w", err)
		}

		msg.Id = result

		if span != nil && span.SpanContext().IsValid() {
			span.SetAttributes(semconv.MessagingMessageID(msg.Id))
		}

		slog.DebugContext(ctx, "added message to stream", "id", msg.Id, "payload", msg.GetPayload())
	}

	return nil
}

// Add adds a new message to the queue
func (mq *MessageQueue) Add(ctx context.Context, message *Message) error {
	if message.GetId() == "" {
		message.Id = uuid.New().String()
	}
	if message.GetBornTimestamp().GetSeconds() == 0 {
		message.BornTimestamp = timestamppb.New(time.Now())
	}
	if message.GetDeliverTimestamp().GetSeconds() == 0 {
		message.DeliverTimestamp = message.GetBornTimestamp()
	}

	message.Topic = mq.opts.Topic

	return mq.enqueueMessage(ctx, mq.opts.Client, message)
}

func (mq *MessageQueue) streamDelayKeyString() string {
	return fmt.Sprintf("%s:delayed", mq.streamString())
}

func (mq *MessageQueue) streamDlqKeyString() string {
	return fmt.Sprintf("%s:dlq", mq.streamString())
}

func (mq *MessageQueue) streamLockKeyString() string {
	return fmt.Sprintf("%s:lock", mq.streamString())
}

func (mq *MessageQueue) streamString() string {
	return fmt.Sprintf("rsmq:{%s}", mq.opts.Topic)
}

func (mq *MessageQueue) streamGroupRateKeyString() string {
	return fmt.Sprintf("%s:%s:rate", mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup)
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
	eg := &errgroup.Group{}
	eg.SetLimit(int(mq.opts.ConsumeOpts.MaxConcurrency))

	return mq.BatchConsume(ctx, func(ctx context.Context, messages []*Message) []error {
		errors := make([]error, len(messages))
		for i, msg := range messages {
			eg.Go(func() error {
				errors[i] = handler(ctx, msg)
				return nil
			})
		}
		_ = eg.Wait()
		return errors
	})
}

// BatchConsume starts consuming messages from the queue in batches
func (mq *MessageQueue) BatchConsume(ctx context.Context, handler BatchMessageHandler) error {
	opts := mq.opts.ConsumeOpts

	if opts.ConsumerGroup == "" {
		return fmt.Errorf("consumer group is required")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if mq.closed.Load() == 1 {
				return nil
			}

			// Consume from stream
			_, err := mq.consumeStream(ctx, handler)
			if err != nil {
				slog.ErrorContext(ctx, "failed to consume from stream", "error", err)
			}

		}
	}
}

// Close closes the message queue
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

// processDelayedMessages processes delayed messages
func (mq *MessageQueue) processDelayedMessages(ctx context.Context) (int, error) {
	now := time.Now().Unix()

	result, err := mq.delayScript.Run(ctx, mq.opts.Client, []string{mq.streamDelayKeyString(), mq.streamString()}, now).Int()
	if err != nil {
		return 0, fmt.Errorf("failed to process delayed messages: %w", err)
	}

	return result, nil
}

// consumeStream consumes messages from the stream
func (mq *MessageQueue) consumeStream(ctx context.Context, handler BatchMessageHandler) (uint32, error) {
	var processed uint32

	// Process normal messages
	normalProcessed, err := mq.processMessages(ctx, handler, false)
	if err != nil {
		return processed, err
	}
	processed += normalProcessed

	// If no new normal messages, try to process pending messages
	if normalProcessed == 0 {
		pendingProcessed, err := mq.processMessages(ctx, handler, true)
		if err != nil {
			slog.ErrorContext(ctx, "failed to process pending messages", "error", err)
		}
		processed += pendingProcessed
	}

	// Process delayed messages
	_, err = mq.processDelayedMessages(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "failed to process delayed messages", "error", err)
	}

	return processed, nil
}

// retrieveMessages retrieves messages from the stream
func (mq *MessageQueue) retrieveMessages(ctx context.Context, isPending bool) ([]redis.XMessage, error) {
	batchSize := mq.opts.ConsumeOpts.BatchSize
	if mq.rateLimit != nil {
		batchSize = mq.doRateLimit(ctx, batchSize)
	}

	var newMessages []redis.XMessage

	if isPending {
		claimed, _, err := mq.opts.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   mq.streamString(),
			Group:    mq.opts.ConsumeOpts.ConsumerGroup,
			Consumer: mq.opts.ConsumeOpts.ConsumerID,
			Start:    "-",
			MinIdle:  mq.opts.ConsumeOpts.PendingTimeout,
			Count:    batchSize,
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to claim pending messages: %w", err)
		}

		newMessages = claimed
	} else {
		// Wait for the next poll, but don't exceed the time until the next delayed message
		nextBlockTime, err := mq.getNextBlockTime(ctx)
		if err != nil {
			return nil, err
		}

		if nextBlockTime == 0 || nextBlockTime > mq.opts.ConsumeOpts.MaxBlockDuration {
			nextBlockTime = mq.opts.ConsumeOpts.MaxBlockDuration
		}

		streams, err := mq.opts.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Streams:  []string{mq.streamString(), ">"},
			Group:    mq.opts.ConsumeOpts.ConsumerGroup,
			Consumer: mq.opts.ConsumeOpts.ConsumerID,
			Count:    batchSize,
			Block:    nextBlockTime,
		}).Result()
		if err != nil {
			if err == redis.Nil {
				return nil, nil // No messages
			}
			if strings.HasPrefix(err.Error(), "NOGROUP") {
				if err := mq.ensureConsumerGroup(ctx, mq.opts.ConsumeOpts.ConsumerGroup); err != nil {
					return nil, err
				}
				return mq.retrieveMessages(ctx, isPending)
			}
			return nil, fmt.Errorf("failed to read from stream: %w", err)
		}

		newMessages = make([]redis.XMessage, 0, batchSize)
		for _, stream := range streams {
			newMessages = append(newMessages, stream.Messages...)
		}
	}

	return newMessages, nil
}

// processMessages retrieves messages from the stream and processes them
func (mq *MessageQueue) processMessages(ctx context.Context, handler BatchMessageHandler, isPending bool) (uint32, error) {
	newMessages, err := mq.retrieveMessages(ctx, isPending)
	if err != nil {
		return 0, err
	}

	messages := make([]*Message, 0, len(newMessages))
	messageIDs, invalidMessageIds := make([]string, 0, len(newMessages)), make([]string, 0, len(newMessages))
	links := make([]trace.Link, 0, len(newMessages))

	for _, message := range newMessages {
		m, err := mq.unmarshalMessage(message)
		if err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "error", err)
			invalidMessageIds = append(invalidMessageIds, message.ID)
			continue
		}
		if mq.opts.ConsumeOpts.SubExpression != "*" {
			if _, ok := mq.subExpressionMap[m.Tag]; !ok {
				invalidMessageIds = append(invalidMessageIds, m.GetId())
				continue
			}
		}
		messages = append(messages, m)
		messageIDs = append(messageIDs, message.ID)
		links = append(links, trace.Link{
			SpanContext: trace.SpanContextFromContext(otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.GetMetadata()))),
			Attributes: []attribute.KeyValue{
				MessagingRsmqMessageTopic.String(mq.opts.Topic),
				MessagingRsmqMessageTag.String(m.GetTag()),
				semconv.MessagingMessageBodySize(len(m.GetPayload())),
				semconv.MessagingMessageID(m.GetId()),
			},
		})
	}

	mq.ackMessages(ctx, invalidMessageIds...)

	if len(messages) == 0 {
		return 0, nil
	}

	if mq.opts.TracerProvider != nil {
		var span trace.Span
		ctx, span = mq.opts.TracerProvider.Tracer("rsmq").Start(ctx, "ConsumeStream",
			trace.WithLinks(links...),
			trace.WithAttributes(
				MessagingRsmqSystem,
				MessagingRsmqMessageTopic.String(mq.opts.Topic),
				MessagingRsmqMessageGroup.String(mq.opts.ConsumeOpts.ConsumerGroup),
				semconv.MessagingBatchMessageCount(len(messages)),
			))
		defer span.End()
	}

	errors := handler(ctx, messages)

	retryMessages := make([]*Message, 0, len(messages))
	successMessageIds := make([]string, 0, len(messages))

	for i, msg := range messages {
		if len(errors) > i && errors[i] != nil {
			retryMessages = append(retryMessages, msg)
		} else {
			successMessageIds = append(successMessageIds, messageIDs[i])
		}
	}

	for _, message := range retryMessages {
		msg := proto.Clone(message).(*Message)
		if atomic.AddUint32(&msg.RetryCount, 1) > mq.opts.ConsumeOpts.MaxRetryLimit {
			// Send to DLQ
			if err := mq.send2dlq(ctx, msg); err != nil {
				slog.ErrorContext(ctx, "failed to send message to dlq", "error", err)
			}
		} else {
			// Retry the message
			if err := mq.retry(ctx, msg); err != nil {
				slog.ErrorContext(ctx, "failed to retry message", "error", err)
			}
		}
	}

	// Acknowledge successfully processed messages
	mq.ackMessages(ctx, successMessageIds...)

	return uint32(len(messages)), nil
}

func (mq *MessageQueue) ackMessages(ctx context.Context, ids ...string) {
	if len(ids) == 0 {
		return
	}

	err := mq.opts.Client.XAck(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, ids...).Err()
	if err != nil {
		slog.ErrorContext(ctx, "failed to ack messages", "error", err)
	}
}

// getRateLimit returns the rate limit for the consumer group
func (mq *MessageQueue) getNextBlockTime(ctx context.Context) (time.Duration, error) {
	// Get the next delayed message time
	result, err := mq.opts.Client.ZRangeWithScores(ctx, mq.streamDelayKeyString(), 0, 0).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get next delayed message time: %w", err)
	}

	if len(result) == 0 {
		return 0, nil
	}

	return time.Until(time.Unix(int64(result[0].Score), 0)), nil
}

// generateConsumerID generates a unique consumer ID
func generateConsumerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// cleanIdleConsumers removes idle consumers from the consumer group
func (mq *MessageQueue) cleanIdleConsumers(ctx context.Context) error {
	consumers, err := mq.opts.Client.XInfoConsumers(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup).Result()
	if err != nil {
		return fmt.Errorf("failed to get consumers: %w", err)
	}

	for _, consumer := range consumers {
		// skip the current stream consumer
		if consumer.Name == mq.opts.ConsumeOpts.ConsumerID {
			continue
		}

		if consumer.Idle > mq.opts.ConsumeOpts.ConsumerIdleTimeout {
			err := mq.opts.Client.XGroupDelConsumer(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, consumer.Name).Err()
			if err != nil {
				return fmt.Errorf("failed to delete consumer: %w", err)
			}
		}
	}

	return nil
}

// cleanIdleMessages removes idle messages from the stream
func (mq *MessageQueue) cleanIdleMessages(ctx context.Context) error {
	minId := fmt.Sprintf("%d", time.Now().Add(-mq.opts.RetentionOpts.MaxRetentionTime).UnixMilli())

	_, err := mq.opts.Client.XTrimMinIDApprox(ctx, mq.streamString(), minId, 1000).Result()
	if err != nil {
		return fmt.Errorf("failed to trim stream: %w", err)
	}

	_, err = mq.opts.Client.XTrimMinIDApprox(ctx, mq.streamDlqKeyString(), minId, 1000).Result()
	if err != nil {
		return fmt.Errorf("failed to trim dlq stream: %w", err)
	}

	return nil
}

// withRedisLock executes a function with a Redis lock
func (mq *MessageQueue) withRedisLock(ctx context.Context, key string, f func(context.Context) error) error {
	lock, err := mq.redisLock.Obtain(ctx, key, 3*time.Second, &redislock.Options{})
	if err != nil {
		return fmt.Errorf("failed to obtain lock: %w", err)
	}
	defer func() { _ = lock.Release(ctx) }()

	return f(ctx)
}

// retry re-queues a message with an exponential backoff
func (q *MessageQueue) retry(ctx context.Context, msg *Message) error {
	// Make the delete and re-queue operation atomic in case we crash midway
	// and lose a message.
	pipe := q.opts.Client.TxPipeline()
	// When retry a msg, ack it before we delete msg.
	if err := pipe.XAck(ctx, q.streamString(), q.opts.ConsumeOpts.ConsumerGroup, msg.GetId()).Err(); err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}

	err := pipe.XDel(ctx, q.streamString(), msg.GetId()).Err()
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	// Set the origin message id if it's not set
	if msg.OriginMsgId == "" {
		msg.OriginMsgId = msg.Id
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

// send2dlq sends a message to the dead-letter queue
func (mq *MessageQueue) send2dlq(ctx context.Context, msg *Message) error {
	// Make the delete and re-queue operation atomic in case we crash midway
	// and lose a message.
	pipe := mq.opts.Client.TxPipeline()
	// When retry a msg, ack it before we delete msg.
	if err := pipe.XAck(ctx, mq.streamString(), mq.opts.ConsumeOpts.ConsumerGroup, msg.GetId()).Err(); err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}

	err := pipe.XDel(ctx, mq.streamString(), msg.GetId()).Err()
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	// Set the origin message id if it's not set
	if msg.OriginMsgId == "" {
		msg.OriginMsgId = msg.Id
	}

	messageBytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Add to dlq stream
	result, err := pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: mq.streamDlqKeyString(),
		MaxLen: mq.opts.RetentionOpts.MaxLen,
		Approx: true,
		Values: map[string]interface{}{"message": string(messageBytes)},
	}).Result()
	if err != nil {
		return fmt.Errorf("failed to add message to dlq stream: %w", err)
	}

	msg.Id = result

	slog.DebugContext(ctx, "added message to dlq stream", "id", msg.Id)

	_, err = pipe.Exec(ctx)
	return err
}

// unmarshalMessage unmarshal a message from Redis
func (mq *MessageQueue) unmarshalMessage(msg redis.XMessage) (*Message, error) {
	messageJSON := msg.Values["message"].(string)
	var m Message
	if err := proto.Unmarshal([]byte(messageJSON), &m); err != nil {
		return nil, fmt.Errorf("error unmarshal message: %w", err)
	}
	m.Id = msg.ID
	return &m, nil
}

// doRateLimit limits the rate of consuming messages
func (mq *MessageQueue) doRateLimit(ctx context.Context, n int64) int64 {
	for {
		result, err := mq.rateLimit.AllowAtMost(ctx,
			mq.streamGroupRateKeyString(), redis_rate.PerSecond(mq.opts.ConsumeOpts.RateLimit), int(n))
		if err != nil {
			slog.ErrorContext(ctx, "failed to rate limit", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if result.Allowed > 0 {
			return int64(result.Allowed)
		}

		time.Sleep(result.RetryAfter)
	}
}

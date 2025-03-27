package jikmq

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
)

// Consumer 消费者
type Consumer struct {
	client *redis.Client
}

func NewConsumer(client *redis.Client) *Consumer {
	c := Consumer{
		client: client,
	}
	return &c
}

// ConsumeMsg 消费一条消息
func (c *Consumer) ConsumeMsg(ctx context.Context, topic, group, consumer string) ([]redis.XStream, error) {
	msgs, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, ">"},
		Count:    1,    // 每次消费一条消息
		Block:    1000, // 阻塞 1000 毫秒
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	return msgs, nil
}

// AckMsg 确认消息
func (c *Consumer) AckMsg(ctx context.Context, topic, group string, ids []string) error {
	return c.client.XAck(ctx, topic, group, ids...).Err()
}

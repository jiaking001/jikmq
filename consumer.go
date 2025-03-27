package jikmq

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
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
func (c *Consumer) ConsumeMsg(ctx context.Context, topic, group, consumer string) ([]redis.XMessage, error) {
	result, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{topic, ">"},
		Count:    1,
		Block:    1000, // 阻塞 1000 毫秒
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// 超时，没有新消息
		}
		log.Fatalf("Failed to read from stream: %v", err)
	}
	return result[0].Messages, nil
}

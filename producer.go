package jikmq

import (
	"context"
	"github.com/go-redis/redis/v8"
)

// Producer 生产者
type Producer struct {
	client *redis.Client
}

func NewProducer(client *redis.Client) *Producer {
	p := Producer{
		client: client,
	}
	return &p
}

// SendMsg 生产一条消息
func (p *Producer) SendMsg(ctx context.Context, topic, key, val string) (string, error) {
	// 使用 XAdd 命令将消息添加到 Redis Stream
	id, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			key: val,
		},
		MaxLen: 10,    // 设置队列的最大容纳量
		Approx: false, // 是否使用近似值，false 表示精确值
	}).Result()
	if err != nil {
		return "", err
	}
	return id, nil
}

package jikmq

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
)

// MsgCallback 接收到消息后执行的回调函数
type MsgCallback func(ctx context.Context, msg redis.XStream) error

// Consumer 消费者
type Consumer struct {
	client *redis.Client

	ctx context.Context

	stop context.CancelFunc

	topic string // Stream 名称

	group string // 消费组

	consumer string // 消费者

	failureCnts map[interface{}]int // 各消息累计失败次数

	callbackFunc MsgCallback // 接收到 msg 时执行的回调函数，由使用方定义
}

func NewConsumer(client *redis.Client, topic, group, consumer string, callbackFunc MsgCallback) *Consumer {
	ctx, stop := context.WithCancel(context.Background())

	c := Consumer{
		client: client,

		ctx: ctx,

		stop: stop,

		topic: topic, // Stream 名称

		group: group, // 消费组

		consumer: consumer, // 消费者

		callbackFunc: callbackFunc, // 接收到 msg 时执行的回调函数，由使用方定义
	}

	go c.run()
	return &c
}

// Stop 停止 consumer
func (c *Consumer) Stop() {
	c.stop()
}

// 监听消息
func (c *Consumer) run() {
	for {
		// select 多路复用
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// 接收最新的消息
		msgs, err := c.consumeMsg()
		if err != nil {
			log.Printf("receive msg failed, err: %v", err)
			continue
		}

		// 接收消息成功后处理消息
		c.handlerMsgs(c.ctx, msgs)
	}

}

// 处理接收到的消息
func (c *Consumer) handlerMsgs(ctx context.Context, msgs []redis.XStream) {
	for _, msg := range msgs {
		// 执行回调函数
		if err := c.callbackFunc(ctx, msg); err != nil {
			// 失败计数器累加
			c.failureCnts[msg.Messages[0].Values["key"]]++
			continue
		}
		// callback 执行成功，进行 ack
		if err := c.ackMsg(msg.Messages[0].ID); err != nil {
			log.Printf("msg ack failed, msg id: %s, err: %v", msg.Messages[0].ID, err)
			continue
		}

		// 消费成功清空计数器
		delete(c.failureCnts, msg.Messages[0].Values["key"])
	}
}

// ConsumeMsg 消费一条消息
func (c *Consumer) consumeMsg() ([]redis.XStream, error) {
	msgs, err := c.client.XReadGroup(c.ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{c.topic, ">"},
		Count:    1,    // 每次消费一条消息
		Block:    1000, // 阻塞 1000 毫秒
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	return msgs, nil
}

// AckMsg 确认消息
func (c *Consumer) ackMsg(id string) error {
	return c.client.XAck(c.ctx, c.topic, c.group, id).Err()
}

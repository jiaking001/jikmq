package jikmq

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

// MsgCallback 接收到消息后执行的回调函数
type MsgCallback func(ctx context.Context, msg MsgEntity) error

// DeadLetterMailbox 死信队列
type DeadLetterMailbox func(ctx context.Context, msg MsgEntity) error

// MsgEntity 消息类
type MsgEntity struct {
	MsgID string
	Val   interface{}
}

// Consumer 消费者
type Consumer struct {
	client *redis.Client

	ctx context.Context

	stop context.CancelFunc

	topic string // Stream 名称

	group string // 消费组

	consumer string // 消费者

	failureCnt map[MsgEntity]int // 各消息累计失败次数

	callbackFunc MsgCallback // 接收到 msg 时执行的回调函数，由使用方定义

	deadLetterMailbox DeadLetterMailbox // 死信队列，由使用方自定义实现
}

// NewConsumer 创建新的消费者
func NewConsumer(client *redis.Client, topic, group, consumer string, callbackFunc MsgCallback, deadLetterMailbox DeadLetterMailbox) *Consumer {
	ctx, stop := context.WithCancel(context.Background())

	c := Consumer{
		client: client,

		ctx: ctx,

		stop: stop, // 用于停止消费

		topic: topic, // Stream 名称

		group: group, // 消费组

		consumer: consumer, // 消费者

		failureCnt: make(map[MsgEntity]int), // 各消息累计失败次数

		callbackFunc: callbackFunc, // 接收到 msg 时执行的回调函数，由使用方定义

		deadLetterMailbox: deadLetterMailbox, // 死信队列，由使用方自定义实现
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

		// 接收最新的消息,没有消息时阻塞防止空转
		msg, err := c.consumeMsg(">", 0)
		if err != nil {
			log.Printf("receive msg failed, err: %v", err)
			continue
		}

		// 接收消息成功后处理消息
		c.handlerMsg(c.ctx, msg)

		// 死信队列投递
		c.deliverDeadLetter(c.ctx)

		// 处理未ACK的老消息
		msg, err = c.consumeMsg("0-0", -1)
		if err != nil {
			log.Printf("receive msg failed, err: %v", err)
			continue
		}

		// 接收消息成功后处理消息
		c.handlerMsg(c.ctx, msg)
	}

}

// 死信队列投递
func (c *Consumer) deliverDeadLetter(ctx context.Context) {
	// 对于失败达到指定次数的消息，投递到死信中，然后执行 ack
	for msg, failureCnt := range c.failureCnt {
		if failureCnt < 2 {
			continue
		}

		// 投递死信队列
		if err := c.deadLetterMailbox(ctx, msg); err != nil {
			log.Printf("dead letter deliver failed, msg id: %s, err: %v", msg, err)
		}

		if err := c.ackMsg(msg.MsgID); err != nil {
			log.Printf("msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
			continue
		}

		// 消费成功清空计数器
		delete(c.failureCnt, msg)
	}
}

// 处理接收到的消息
func (c *Consumer) handlerMsg(ctx context.Context, msg MsgEntity) {
	if msg.MsgID == "" && msg.Val == nil {
		// 没有消息
		return
	}
	// 执行回调函数
	if err := c.callbackFunc(ctx, msg); err != nil {
		// 失败计数器累加
		c.failureCnt[msg]++
		return
	}
	// callback 执行成功，进行 ack
	if err := c.ackMsg(msg.MsgID); err != nil {
		log.Printf("msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
		return
	}

	// 消费成功清空计数器
	delete(c.failureCnt, msg)
}

// ConsumeMsg 消费一条消息
func (c *Consumer) consumeMsg(s string, m time.Duration) (MsgEntity, error) {
	msg, err := c.client.XReadGroup(c.ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  []string{c.topic, s},
		Count:    1, // 每次消费一条消息
		Block:    m, // 阻塞
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return MsgEntity{}, err
	}

	if len(msg) == 0 || len(msg[0].Messages) == 0 {
		return MsgEntity{}, nil
	}

	return MsgEntity{
		MsgID: msg[0].Messages[0].ID,
		Val:   msg[0].Messages[0].Values["key"],
	}, nil
}

// AckMsg 确认消息
func (c *Consumer) ackMsg(id string) error {
	return c.client.XAck(c.ctx, c.topic, c.group, id).Err()
}

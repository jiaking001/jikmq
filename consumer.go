package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
)

func main() {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6350", // Redis 服务器地址
		Password: "",               // 密码（如果有的话）
		DB:       0,                // 使用默认数据库
	})

	// Stream 名称和消费者组名称
	streamName := "myStream"
	groupName := "myGroup"
	consumerName := "consumer1"

	// 创建消费者组（如果尚未创建）
	_, err := client.XGroupCreate(context.Background(), streamName, groupName, "$").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	// 消费消息
	for {
		messages, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			Block:    1000, // 阻塞 1000 毫秒
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				// 超时，没有新消息
				continue
			}
			log.Fatalf("Failed to read from stream: %v", err)
		}

		for _, msg := range messages[0].Messages {
			fmt.Printf("Consumed: %s\n", msg.Values["message"])
			// 确认消息
			_, err = client.XAck(context.Background(), streamName, groupName, msg.ID).Result()
			if err != nil {
				log.Fatalf("Failed to ack message: %v", err)
			}
		}
	}
}

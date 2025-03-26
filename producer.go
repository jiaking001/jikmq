package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

func main() {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6350", // Redis 服务器地址
		Password: "",               // 密码（如果有的话）
		DB:       0,                // 使用默认数据库
	})

	// Stream 名称
	streamName := "myStream"

	// 发送消息
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Message %d", i)
		_, err := client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: streamName,
			Values: map[string]interface{}{
				"message": message,
			},
		}).Result()
		if err != nil {
			log.Fatalf("Failed to add message to stream: %v", err)
		}
		fmt.Printf("Produced: %s\n", message)
		time.Sleep(1 * time.Second)
	}
}

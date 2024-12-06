package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/net/context"
)

func main() {
	// 从命令行参数获取配置项
	addr := flag.String("addr", "localhost:6379", "Redis server address")
	password := flag.String("password", "", "Redis password (empty means no password)")
	count := flag.Int("count", 100, "Number of keys to scan per iteration")
	flag.Parse()

	// 创建 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     *addr,     // 使用命令行指定的 Redis 地址
		Password: *password, // 使用命令行指定的密码
	})

	// 创建上下文
	ctx := context.Background()

	// 打开日志文件
	expiredLog, err := os.OpenFile("expired_keys.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open expired_keys.log: %v", err)
	}
	defer expiredLog.Close()

	notExpiredLog, err := os.OpenFile("not_expired_keys.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open not_expired_keys.log: %v", err)
	}
	defer notExpiredLog.Close()

	noTTLLog, err := os.OpenFile("no_ttl_keys.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open no_ttl_keys.log: %v", err)
	}
	defer noTTLLog.Close()

	// 统计未设置过期时间和过期键的总大小
	var totalSizeNoTTL int64
	var totalSizeExpired int64
	var totalSizeNoExpired int64

	// 捕获终止信号（如 Ctrl+C）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 游标初始化
	cursor := uint64(0)

	go func() {
		<-sigChan
		// 在收到终止信号时打印统计信息
		log.Printf("Total size of keys with no TTL: %d bytes\n", totalSizeNoTTL)
		log.Printf("Total size of expired keys: %d bytes\n", totalSizeExpired)
		log.Printf("Total size of no expired keys: %d bytes\n", totalSizeNoExpired)

		// 确保在程序退出之前关闭 Redis 客户端
		rdb.Close()
		os.Exit(0) // 退出程序
	}()

	for {
		// 使用 SCAN 遍历所有键
		keys, newCursor, err := rdb.Scan(ctx, cursor, "*", int64(*count)).Result()
		if err != nil {
			log.Fatalf("SCAN failed: %v", err)
		}

		// 遍历当前批次的所有键
		for _, key := range keys {
			// 获取键的 TTL（过期时间）
			ttl, err := rdb.TTL(ctx, key).Result()
			if err != nil {
				log.Printf("Failed to get TTL for key %s: %v", key, err)
				continue
			}

			// 获取键的大小
			size, err := rdb.MemoryUsage(ctx, key).Result()
			if err != nil {
				log.Printf("Failed to get memory usage for key %s: %v", key, err)
				continue
			}

			// 判断该键的 TTL
			if ttl < 0 {
				// TTL < 0 表示该键没有过期时间
				log.Printf("Key: %s does not have an expiration time.\n", key)
				// 将没有过期时间的键写入日志
				_, _ = noTTLLog.WriteString(fmt.Sprintf("Key: %s, Size: %d bytes\n", key, size))
				totalSizeNoTTL += size
			} else if ttl == 0 {
				// TTL == 0 表示键已经过期
				log.Printf("Key: %s has expired.\n", key)
				// 将已过期的键写入日志
				_, _ = expiredLog.WriteString(fmt.Sprintf("Key: %s, Size: %d bytes\n", key, size))
				totalSizeExpired += size
			} else {
				// TTL > 0 表示该键尚未过期
				log.Printf("Key: %s has a TTL of %v seconds.\n", key, ttl.Seconds())
				// 将未过期的键写入日志
				_, _ = notExpiredLog.WriteString(fmt.Sprintf("Key: %s, TTL: %v seconds, Size: %d bytes\n", key, ttl.Seconds(), size))
				totalSizeNoExpired += size
			}
		}

		// 如果游标为 0，表示遍历完成
		if newCursor == 0 {
			break
		}
		// 更新游标
		cursor = newCursor
		time.Sleep(200 * time.Millisecond)
	}

	// 输出统计信息
	log.Printf("Total size of keys with no TTL: %d bytes\n", totalSizeNoTTL)
	log.Printf("Total size of expired keys: %d bytes\n", totalSizeExpired)
	log.Printf("Total size of no expired keys: %d bytes\n", totalSizeNoExpired)

	// 关闭 Redis 客户端
	defer rdb.Close()
}

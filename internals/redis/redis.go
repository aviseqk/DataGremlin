package redis

import (
	"context"
	"encoding/json"
	"fmt"
	getenv "github.com/joho/godotenv"
	redis "github.com/redis/go-redis/v9"
	"log"
	"os"
	"time"

	"datagremlin/internals/datamodels"
)

var CacheKeyMap = map[string]string{
	"dev":  "cache:replication_state:dev",
	"prod": "cache:replication_state:prod",
	"test": "cache:replication_state:test",
}

var ttl_expiration_time = 2 * 48 * time.Hour

func SaveLSNCacheToRedis(ctx context.Context, rdb *redis.Client, msg models.RedisCache) error {
	// serialize to JSON
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Unable to marshal Redis Cache: %v ", err)
	}

	// store in Redis Cache
	err = rdb.Set(ctx, CacheKeyMap["dev"], data, ttl_expiration_time).Err()
	if err != nil {
		log.Fatalf("failed to save cache: %v", err)
	}
	fmt.Printf("[SUCCESS] Saved LSN Cache to Redis\n")

	return nil
}

func GetSavedLSNCache(ctx context.Context, rdb *redis.Client) (models.RedisCache, error) {
	val, err := rdb.Get(ctx, CacheKeyMap["dev"]).Result()
	if err != nil {
		log.Fatalf("failed to get cache: %v", err)
	}

	var restored models.RedisCache
	if err := json.Unmarshal([]byte(val), &restored); err != nil {
		log.Fatalf("failed to unmarshal cache: %v", err)
		return models.RedisCache{}, err
	}

	fmt.Printf("[SUCCESS] Restored struct: %+v\n", restored)

	return restored, nil
}

func GetRedisClient(ctx context.Context) (rdb *redis.Client, err error) {
	_ = getenv.Load()

	rdb = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	// test the connection
	err = rdb.Set(ctx, "test-key", "test-value", 0).Err()
	if err != nil {
		log.Panicf("Unable to set values in Redis, connection not proper: %v\n", err)
		return nil, err
	}

	return rdb, nil
}

func TestRedisConnectivity(rdb *redis.Client, ctx context.Context) (bool, error) {

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		return false, err
	}

	if pong == "PONG" {
		return true, nil
	}

	return false, nil
}

func GetValue(rdb *redis.Client, key string, ctx context.Context) (string, error) {
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		log.Default().Printf("[REDIS]: unable to get value for key: %s - Error: %v\n", key, err)
		return "", err
	}

	return val, nil

}

func SetValue(rdb *redis.Client, key string, value string, ctx context.Context) (bool, error) {
	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Default().Printf("[REDIS]: unable to set value for key: %s value:%s - Error: %v\n", key, value, err)
		return false, err
	}
	return true, nil
}

package main

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/lea21st/rmq"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	log := rmq.DefaultLog
	broker := rmq.NewRedisBroker(rdb, rmq.DefaultRedisBrokerConfig, log)
	queue := rmq.NewRmq(broker)

	ctx, exitFuc := context.WithCancel(context.TODO())
	defer exitFuc()
	queue.Start(ctx)

}

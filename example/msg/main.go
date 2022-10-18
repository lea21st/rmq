package main

import (
	"fmt"

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
	fmt.Println(queue)
}

package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lea21st/rmq"
)

var queue *rmq.Rmq

func main() {
	Init()
	queue.StartWorker(&rmq.WorkerConfig{
		WorkerNum:    1,
		Concurrent:   2,
		WaitDuration: time.Second,
	})
	// defer queue.Exit()

	fmt.Println("111")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	fmt.Println("1")
	err := http.ListenAndServe(":8080", nil)
	fmt.Println(err)
}

func Init() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	log := rmq.DefaultLog
	broker := rmq.NewRedisBroker(rdb, rmq.DefaultRedisBrokerConfig, log)
	queue = rmq.NewRmq(broker, log)
	queue.RegisterFunc("测试", Test)

	queue.Hook.OnRun(func(ctx context.Context, r *rmq.TaskRuntime) {
		fmt.Println("协程数量:", runtime.NumGoroutine())
	})
}

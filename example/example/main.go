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
		WorkerNum:    2,
		Concurrent:   20,
		WaitDuration: time.Second,
	})
	// defer queue.Exit()

	time.AfterFunc(20*time.Second, func() {
		queue.Exit()
	})

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
	queue.RegisterFunc("test1", Test1)
	queue.RegisterFunc("test2", Test2)
	queue.Register(&TestTask{})

	queue.Hook.OnRun(func(ctx context.Context, r *rmq.TaskRuntime) error {
		fmt.Println("协程数量:", runtime.NumGoroutine())
		return nil
	})

	queue.Hook.OnComplete(func(ctx context.Context, r *rmq.TaskRuntime) error {
		fmt.Println("协程数量:", runtime.NumGoroutine())
		return nil
	})

	queue.Hook.OnContext(func(ctx context.Context) context.Context {
		return context.WithValue(ctx, "x", 1)
	})
}

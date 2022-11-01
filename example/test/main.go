package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/go-redis/redis/v8"
	"github.com/lpong/rmq"
)

var queue *rmq.Rmq

func main() {
	Init()
	queue.StartWorker(&rmq.WorkerConfig{
		WorkerNum:  2,
		Concurrent: 2,
	})
	// defer queue.Exit()

	// time.AfterFunc(20*time.Second, func() {
	// 	queue.Exit()
	// })

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
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
		fmt.Println("任务开始:", runtime.NumGoroutine())
		return nil
	})

	queue.Hook.OnComplete(func(ctx context.Context, r *rmq.TaskRuntime) error {
		fmt.Println("任务结束:", runtime.NumGoroutine())
		fmt.Println(rmq.Json(r))
		panic("rmq OnComplete")
		return nil
	})

	queue.Hook.OnContext(func(ctx context.Context, msg *rmq.TaskRuntime) context.Context {
		return context.WithValue(ctx, "x", msg.Msg.Meta.TraceId)
	})
}

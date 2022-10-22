# Rmq

## 概述

一个用go写的redis消息队列

## Import

`go get github.com/lea21st/rmq`

## 创建Task
```
// Test1 ,仅一个函数
func Test1(ctx context.Context, msg *rmq.Message) (result string, err error) {
	return
}

// TestTask，实现 rmq.Task
type TestTask struct {
	Name string `json:"name"`
	Val  int    `json:"val"`
}
func (t *TestTask) TaskName() string {
	// TODO implement me
	return "TestTask"
}

func (t *TestTask) Run(ctx context.Context) (result string, err error) {
	return "ok", nil
}

// 自带的Task
rmq.CommandTask 执行一个系统命令
rmq.HttpTask: 执行一个http请求

// 定义解析task的数据，默认使用json.Unmarshal()
type TaskScanner interface {
    Scan(src []byte) error
}

// 定义task的数据序列号方式，默认json.Marshal()
type TaskValuer interface {
    Value() ([]byte, error)
}

// OnLoad 加载时执行的方法
type OnLoad interface {
    Load(ctx context.Context, msg *Message) error
}

// OnSuccess 执行成功时执行的方法
type OnSuccess interface {
    OnSuccess(ctx context.Context)
}

// OnFail 执行失败时执行的方法
type OnFail interface {
    OnFail(ctx context.Context)
}

// OnComplete 执行完成时会调用
type OnComplete interface {
    OnComplete(ctx context.Context)
}

```

## 创建消息
```
msg, err := rmq.NewMsg().SetTask(&TestTask{
    Name: "name-1",
    Val:  1,
})
// 消息定制
msg := rmq.NewMsg() // OR msg := rmq.NewBlankMsg()
msg.SetMeta(rmq.RetryMeta).SetDelay(3 * time.Minute).SetMaxRetry(1).SetTraceId("traceId").SetTimeout(30 * time.Second).SetExpire(30 * time.Second).SetExpiredAt(time.Now().Add(1 * time.Hour))

// 该消息要执行的任务
msg.SetCallback("test1", map[string]any{"x": 1})
// or 
msg.SetTask(&TestTask{
    Name: fmt.Sprintf("testTask-%d", i),
    Val:  i * i,
})

queue.Push(msg)
```

## 创建队列

```
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "",
    DB:       0,
})

log := rmq.DefaultLog
broker := rmq.NewRedisBroker(rdb, rmq.DefaultRedisBrokerConfig, log)
queue = rmq.NewRmq(broker, log)
queue.RegisterFunc("test1", Test1)
queue.Register(&TestTask{})
```

## 消费
```
queue.StartWorker(&rmq.WorkerConfig{
    WorkerNum:  2,
    Concurrent: 2,
})
```

## Rmq Hook
```

// 可选，返回false将不会push消息
queue.Hook.OnPush(func(ctx context.Context, msg ...*rmq.Message) ([]*rmq.Message, error) {
    return msg,nil
})

// 任务开始执行时调用，返回error将取消任务执行
queue.Hook.OnRun(func(ctx context.Context, r *rmq.TaskRuntime) error {
	fmt.Println("任务开始:", runtime.NumGoroutine())
	return nil
})

// 任务执行完成时调用
queue.Hook.OnComplete(func(ctx context.Context, r *rmq.TaskRuntime) error {
    fmt.Println("任务结束:", runtime.NumGoroutine())
    fmt.Println(rmq.Json(r))
    return nil
})

// 任务开始，创建Context时调用
queue.Hook.OnContext(func(ctx context.Context, msg *rmq.TaskRuntime) context.Context {
    return context.WithValue(ctx, "x", msg.Msg.Meta.TraceId)
})
```

## Task Hook
```
func (t *TestTask) OnSuccess(ctx context.Context) {
	fmt.Println(t.Name, "Success hook")
}

func (t *TestTask) OnFail(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Fail hook")
}

func (t *TestTask) OnComplete(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Complete hook")
}
```
package rmq

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type WorkerConfig struct {
	// 配置信息
	WorkerNum    int
	Concurrent   int
	WaitDuration time.Duration
}
type Rmq struct {

	// 队列退出信号
	concurrentChan chan int
	msgChan        chan *Message

	log Logger

	// 关闭方法
	exitFunc context.CancelFunc

	// 执行引擎
	broker  Broker
	process Process

	// 钩子
	Hook *Hook

	// 注册器
	register *Register

	workerWg sync.WaitGroup
}

// NewRmq 创建新队列
func NewRmq(broker Broker, logger Logger) *Rmq {
	rmq := &Rmq{
		broker:   broker,
		process:  NewDefaultProcess(),
		Hook:     &Hook{},
		msgChan:  make(chan *Message),
		workerWg: sync.WaitGroup{},
		register: &register,
		log:      logger,
	}

	rmq.Register("simpleTask", &SimpleTask{})
	rmq.Register("commandTask", &CommandTask{})
	rmq.Register("httpTask", &HttpTask{})
	return rmq
}

func (q *Rmq) Register(name string, task Task) {
	q.register.Register(name, task)
}

func (q *Rmq) RegisterFunc(name string, callback Callback) {
	q.register.Register(name, callback)
}

// SetProcess 自定义处理引擎
func (q *Rmq) SetProcess(p Process) {
	q.process = p
}

// SetBroker 自定义处理引擎
func (q *Rmq) SetBroker(b Broker) {
	q.broker = b
}

// StartWorker 线上是多容器的，不用多个协程并发跑,只要加pod就行
// 某前理论存在丢失消息的可能，所以只能用于不重要的任务
func (q *Rmq) StartWorker(c *WorkerConfig) {
	ctx := context.Background()
	ctx, q.exitFunc = context.WithCancel(ctx)

	// before start Hook
	if impl, ok := q.broker.(BrokerHook); ok {
		impl.BeforeStart()
	}

	// start
	q.workerWg.Add(c.WorkerNum * c.Concurrent)
	for i := 0; i < c.WorkerNum; i++ {
		go func(workerId int) {
			fmt.Printf("rmq worker-%d start", workerId)
			for {
				select {
				case <-ctx.Done():
					q.log.Infof("rmq worker-%d exit", workerId)
					return
				default:
					func() {
						q.workerWg.Add(1)
						ctx := context.TODO()
						var msg *Message
						var err error
						if msg, err = q.broker.Pop(ctx); err != nil {
							q.log.Errorf("获取消息失败：", err)
						}
						fmt.Println("msg", msg, err)
						if msg == nil {
							q.workerWg.Done()
							time.Sleep(1 * time.Second)
							return
						}
						q.TryRun(ctx, msg)
					}()
				}
			}
		}(i)
	}

	// after start Hook
	if impl, ok := q.broker.(BrokerHook); ok {
		impl.AfterStart()
	}
}

// Exit 退出
func (q *Rmq) Exit() {
	q.log.Infof("rmq 开始退出")
	q.exitFunc()
	q.workerWg.Wait()
	q.log.Infof("rmq 退出成功")
}

// Push 写入消息到队列
func (q *Rmq) Push(ctx context.Context, msg ...*Message) (v int64, err error) {
	if q.Hook.onPush != nil {
		if msg, err = q.Hook.onPush(ctx, msg...); err != nil {
			return
		}
	}
	v, err = q.broker.Push(ctx, msg...)
	return
}

func (q *Rmq) addConcurrent() {
	q.concurrentChan <- 1
}

func (q *Rmq) doneConcurrent() {
	<-q.concurrentChan
}

// TryRun 解析消息，执行
func (q *Rmq) TryRun(ctx context.Context, msg *Message) {
	defer q.workerWg.Done()
	var cancelFunc context.CancelFunc
	if msg.Meta.Timeout == 0 {
		msg.Meta.Timeout = 30
	}

	runtime := NewTaskRuntime(msg)

	// run Hook
	if q.Hook.onRun != nil {
		q.Hook.onRun(ctx, runtime)
	}

	ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(msg.Meta.Timeout)*time.Second)
	go func() {
		var err error
		defer cancelFunc()
		defer func() {
			rErr := recover()
			if rErr == nil && runtime.RunErr == nil {
				return
			}

			if err := recover(); err != nil {
				runtime.Error = fmt.Errorf("任务%s panic: %v", msg.Id, err)
				q.log.Errorf("任务%s panic: %v，将到下一个时间点重试", msg.Id, err)
			}

			if runtime.Error == nil {
				return
			}

			// 有错误，就重试
			if runtime.Error = q.TryRetry(ctx, msg); runtime.Error != nil {
				q.log.Errorf("任务%s重试失败,%s", msg.Id, runtime.Error)
			} else {
				q.log.Infof("任务%s重试成功,将在%s开始重试", msg.Id, msg.RunAt.DateTime())
			}

			// retry Hook
			if q.Hook.onRetry != nil {
				q.Hook.onRetry(ctx, runtime)
			}

		}()

		// 执行
		var result string
		if result, err = q.process.Exec(ctx, runtime); err != nil {
			q.log.Errorf("任务%s执行失败,%s", msg.Id, err)
			runtime.RunErr = err
			runtime.Error = err
			return
		}
		// 执行成功
		q.log.Infof("任务%s执行成功,Result %s", msg.Id, result)

		// completed Hook
		if q.Hook.onComplete != nil {
			q.Hook.onComplete(ctx, runtime)
		}
		return
	}()
	<-ctx.Done()
}

// TryRetry 尝试重试
func (q *Rmq) TryRetry(ctx context.Context, msg *Message) (err error) {
	if msg.Meta.Retry[0] >= msg.Meta.Retry[1] {
		err = fmt.Errorf("已达到最大重试次数[%d/%d]", msg.Meta.Retry[0], msg.Meta.Retry[1])
		return
	}

	msg.Meta.Retry[0]++
	index := msg.Meta.Retry[0] - 1
	if index >= len(msg.Meta.RetryRule) {
		index = len(msg.Meta.RetryRule) - 1 // 没有规则取最后一个
	}

	delay := msg.Meta.RetryRule[index]
	msg.TryRetry(time.Duration(delay) * time.Second)
	if msg.RunAt > msg.ExpiredAt {
		err = fmt.Errorf("任务在下个时间点重试将过期，取消重试，过期时间%s", msg.ExpiredAt.DateTime())
		return
	}
	_, err = q.Push(ctx, msg)
	return
}

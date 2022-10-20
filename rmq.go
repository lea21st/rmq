package rmq

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
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
	concurrentChan chan struct{}

	log Logger

	// 关闭方法
	exitFunc context.CancelFunc

	// 执行引擎
	broker  Broker
	process Process

	// 钩子
	Hook *Hook

	workerWg sync.WaitGroup
}

// NewRmq 创建新队列
func NewRmq(broker Broker, logger Logger) *Rmq {
	rmq := &Rmq{
		broker:   broker,
		process:  NewDefaultProcess(),
		Hook:     &Hook{},
		workerWg: sync.WaitGroup{},
		log:      logger,
	}
	// 注册自带的task
	rmq.Register(&CommandTask{}, &HttpTask{})
	return rmq
}

func (q *Rmq) RegisterFunc(name string, callback Callback) {
	register.RegisterFunc(name, callback)
}

func (q *Rmq) Register(task ...Task) {
	for i := range task {
		register.Register(task[i])
	}
}

func (q *Rmq) Tasks() map[string]*TaskInfo {
	return register.AllTask()
}

// SetProcess 自定义处理引擎
func (q *Rmq) SetProcess(p Process) {
	q.process = p
}

// SetBroker 自定义处理引擎
func (q *Rmq) SetBroker(b Broker) {
	q.broker = b
}

func (q *Rmq) setConcurrent(num int) {
	q.concurrentChan = make(chan struct{}, num)
}

func (q *Rmq) addConcurrent() {
	q.concurrentChan <- struct{}{}
}

func (q *Rmq) doneConcurrent() {
	<-q.concurrentChan
}

// StartWorker 线上是多容器的，不用多个协程并发跑,只要加pod就行
// 某前理论存在丢失消息的可能，所以只能用于不重要的任务
func (q *Rmq) StartWorker(c *WorkerConfig) {
	ctx := context.Background()
	ctx, q.exitFunc = context.WithCancel(ctx)

	// before start Hook
	if impl, ok := q.broker.(BrokerBeforeStart); ok {
		if err := BrokerHookProtect(ctx, impl.BeforeStart); err != nil {
			q.log.Errorf("broker before start hook exec fail ,err:%s", err)
		}
	}

	// start
	q.setConcurrent(c.Concurrent)
	for i := 0; i < c.WorkerNum; i++ {
		go func(ctx context.Context, workerId int) {
			defer func() {
				if err := recover(); err != nil {
					debug.PrintStack()
					log.Printf("任务异常:%s,将在%s后重启", err, time.Minute)
					// 等一会重启
					time.AfterFunc(time.Minute, func() {
						q.startWorker(ctx, workerId)
					})
				}
			}()
			q.startWorker(ctx, workerId)
		}(ctx, i)
	}

	// after start Hook
	if impl, ok := q.broker.(BrokerAfterStart); ok {
		if err := BrokerHookProtect(ctx, impl.AfterStart); err != nil {
			q.log.Errorf("broker after start hook exec fail ,err:%s", err)
		}
	}
}

func (q *Rmq) startWorker(ctx context.Context, workerId int) {
	q.log.Infof("rmq worker-%d started", workerId)
	for {
		select {
		case <-ctx.Done():
			q.log.Infof("rmq worker-%d exited", workerId)
			return
		default:
			func() {
				q.addConcurrent()
				ctx := context.TODO()
				var msg *Message
				if msg, _ = q.broker.Pop(ctx); msg == nil {
					q.doneConcurrent()
					time.Sleep(1 * time.Second)
					return
				}

				// 开始执行任务
				go func(ctx context.Context, msg *Message) {
					q.workerWg.Add(1)
					defer q.workerWg.Done()
					defer q.doneConcurrent()
					q.TryRun(ctx, msg)
				}(ctx, msg)
			}()
		}
	}
}

// Exit 退出
func (q *Rmq) Exit() {
	q.log.Infof("rmq 开始退出")

	// broker
	if impl, ok := q.broker.(BrokerBeforeExit); ok {
		if err := Protect(func() error {
			return impl.BeforeExit(context.TODO())
		}); err != nil {
			q.log.Errorf("BrokerBeforeExit 执行失败,err:%s", err)
		}
	}

	q.exitFunc()
	q.log.Infof("rmq 已停止消费消息")
	for len(q.concurrentChan) > 0 {
		q.log.Infof("rmq 正在等待%d个任务执行结束", len(q.concurrentChan))
		time.Sleep(time.Second)
	}
	q.workerWg.Wait()
	q.log.Infof("rmq 所有任务已执行完成")

	// broker
	if impl, ok := q.broker.(BrokerAfterExit); ok {
		if err := Protect(func() error {
			return impl.AfterExit(context.TODO())
		}); err != nil {
			q.log.Errorf("BrokerBeforeExit 执行失败,err:%s", err)
		}
	}
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

// TryRun 解析消息，执行
func (q *Rmq) TryRun(ctx context.Context, msg *Message) {
	taskRuntime := NewTaskRuntime(msg)
	taskRuntime.StartTime = time.Now()
	defer func() {
		taskRuntime.EndTime = time.Now()
		taskRuntime.Duration = taskRuntime.EndTime.Sub(taskRuntime.StartTime)
	}()

	// 完成 hook
	defer func() {
		// complete Hook
		if q.Hook.onComplete != nil {
			if err := Protect(func() error {
				return q.Hook.onComplete(ctx, taskRuntime)
			}); err != nil {
				q.log.Errorf("Hook.onComplete 执行失败,任务ID: %s,error: %s", msg.Id, err)
			}
		}
	}()

	// 失败重试
	defer func() {
		// 失败重试
		if taskRuntime.TaskError != nil {
			taskRuntime.Error = taskRuntime.TaskError
			// 重试
			if taskRuntime.Error = q.TryRetry(ctx, msg); taskRuntime.Error != nil {
				q.log.Errorf("任务%s[%d/%d]重试失败,%s", msg.Id, msg.Meta.Retry[0], msg.Meta.Retry[1], taskRuntime.Error)
			} else {
				q.log.Infof("任务%s[%d/%d]重试成功,将在%s开始重试", msg.Id, msg.Meta.Retry[0], msg.Meta.Retry[1], msg.RunAt.DateTime())
			}
		}
	}()

	if q.Hook.onContext != nil {
		if err := Protect(func() error {
			ctx = q.Hook.onContext(ctx, taskRuntime)
			return nil
		}); err != nil {
			q.log.Errorf("Hook.onContext 执行失败,任务ID: %s,error: %s", msg.Id, err)
		}
	}

	// run Hook ,注意执行失败，将不加继续执行下去
	if q.Hook.onRun != nil {
		if err := Protect(func() error {
			return q.Hook.onRun(ctx, taskRuntime)
		}); err != nil {
			taskRuntime.TaskError = err // 这个也需要重试
			q.log.Errorf("Hook.onRun 执行失败,任务ID: %s,error: %s", msg.Id, err)
			return
		}
	}

	// 执行
	if taskRuntime.TaskError = Protect(func() error {
		return q.process.Exec(ctx, taskRuntime)
	}); taskRuntime.TaskError != nil {
		return
	}

	q.log.Infof("任务%s执行成功,Result %v", msg.Id, taskRuntime.Result)

}

// TryRetry 尝试重试
func (q *Rmq) TryRetry(ctx context.Context, msg *Message) (err error) {
	if msg.Meta.Retry[0] >= msg.Meta.Retry[1] {
		err = fmt.Errorf("已达到最大重试次数[%d/%d]", msg.Meta.Retry[0], msg.Meta.Retry[1])
		return
	}

	retry := msg.Meta.Retry[0]
	retry++
	index := retry - 1

	// 这样就不怕忘了设置规则了
	if len(msg.Meta.RetryRule) == 0 {
		msg.Meta.RetryRule = DefaultRetryRule
	}

	if index >= len(msg.Meta.RetryRule) {
		index = len(msg.Meta.RetryRule) - 1 // 没有规则取最后一个
	}

	delay := msg.Meta.RetryRule[index]
	msg.TryRetry(time.Duration(delay) * time.Second)
	if msg.RunAt > msg.ExpiredAt {
		err = fmt.Errorf("任务在下个时间点重试将过期，取消重试，过期时间%s", msg.ExpiredAt.DateTime())
		return
	}

	// 成功后更新重试次数
	if _, err = q.Push(ctx, msg); err != nil {
		msg.Meta.Retry[0] = retry
	}
	return
}

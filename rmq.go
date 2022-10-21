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
	WorkerNum  int
	Concurrent int
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
		process:  newDefaultProcess(logger),
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
					log.Printf("worker exception:%s, will restart after %s", err, time.Minute)
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
	q.log.Infof("rmq exiting")

	// broker
	if impl, ok := q.broker.(BrokerBeforeExit); ok {
		if err := Protect(func() error {
			return impl.BeforeExit(context.TODO())
		}); err != nil {
			q.log.Errorf("BrokerBeforeExit execution failed, error:%s", err)
		}
	}

	q.exitFunc()
	q.log.Infof("rmq stopped receiving messages")
	for len(q.concurrentChan) > 0 {
		q.log.Infof("rmq waiting for %s task to finish execution", len(q.concurrentChan))
		time.Sleep(time.Second)
	}
	q.workerWg.Wait()
	q.log.Infof("rmq all tasks have been completed")

	// broker
	if impl, ok := q.broker.(BrokerAfterExit); ok {
		if err := Protect(func() error {
			return impl.AfterExit(context.TODO())
		}); err != nil {
			q.log.Errorf("BrokerBeforeExit run failed, err:%s", err)
		}
	}
	q.log.Infof("rmq exited")
}

// Push 写入消息到队列
func (q *Rmq) Push(ctx context.Context, msg ...*Message) (err error) {
	if q.Hook.onPush != nil {
		if msg, err = q.Hook.onPush(ctx, msg...); err != nil {
			return
		}
	}
	err = q.broker.Push(ctx, msg...)
	return
}

// TryRun 解析消息，执行
func (q *Rmq) TryRun(ctx context.Context, msg *Message) {
	taskRuntime := newTaskRuntime(msg)
	taskRuntime.StartTime = time.Now()
	q.log.Infof("%s start execution, Id:%s, Data: %s", msg.Task, msg.Id, string(msg.Data))

	defer func() {
		// Rmq.Complete Hook
		if q.Hook.onComplete != nil {
			if err := Protect(func() error {
				return q.Hook.onComplete(ctx, taskRuntime)
			}); err != nil {
				q.log.Errorf("failed to run Rmq.Hook.onComplete,Id: %s, Error: %s", msg.Id, err)
			}
		}
	}()

	defer func() {
		// statistical data
		taskRuntime.EndTime = time.Now()
		taskRuntime.Duration = taskRuntime.EndTime.Sub(taskRuntime.StartTime)
	}()

	// Judge whether it is expired. Please do not retry
	if msg.ExpiredAt < Now() {
		taskRuntime.TaskError = fmt.Errorf("task %s(%s) is expired on %s", msg.Task, msg.Id, msg.ExpiredAt.DateTime())
		q.log.Errorf("failed to run %s, id:%s, error: %s", msg.Task, msg.Id, taskRuntime.TaskError)
		taskRuntime.Error = taskRuntime.TaskError
		return
	}

	defer func() {
		// If failed, retry
		if taskRuntime.TaskError != nil {
			if taskRuntime.Error = q.TryRetry(ctx, msg); taskRuntime.Error != nil {
				q.log.Errorf("%s[%d/%d] retry failed, id:%s, error:%s", msg.Task, msg.Meta.Retry[0], msg.Meta.Retry[1], msg.Id, taskRuntime.Error)
			} else {
				q.log.Infof("%s[%d/%d] retry succeeded, id:%s, will retry on %s", msg.Task, msg.Meta.Retry[0], msg.Meta.Retry[1], msg.Id, msg.RunAt.DateTime())
			}
		}
	}()

	// Rmq.Hook.onContext
	if q.Hook.onContext != nil {
		if err := Protect(func() error {
			ctx = q.Hook.onContext(ctx, taskRuntime)
			return nil
		}); err != nil {
			q.log.Errorf("failed to run Rmq.Hook.onContext, id: %s, error: %s", msg.Id, err)
		}
	}

	//  Rmq.Hook.onRun ,注意执行失败，将不加继续执行下去
	if q.Hook.onRun != nil {
		if err := Protect(func() error {
			return q.Hook.onRun(ctx, taskRuntime)
		}); err != nil {
			taskRuntime.TaskError = err // 这个也需要重试
			taskRuntime.Error = err
			q.log.Errorf("failed to run Rmq.Hook.onRun, the task %s will be canceled, id: %s, error: %s", msg.Task, msg.Id, err)
			return
		}
	}

	// run
	if taskRuntime.TaskError = Protect(func() error {
		return q.process.Exec(ctx, taskRuntime)
	}); taskRuntime.TaskError != nil {
		taskRuntime.Error = taskRuntime.TaskError
		q.log.Errorf("failed to run %s, id:%s, error: %s", msg.Task, msg.Id, taskRuntime.TaskError)
	} else {
		q.log.Infof("the %s executed successfully, id:%s, result: %s", msg.Task, msg.Id, taskRuntime.Result)
	}
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
	if err = q.Push(ctx, msg); err != nil {
		msg.Meta.Retry[0] = retry
	}
	return
}

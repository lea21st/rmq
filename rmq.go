package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type Config struct {
	Key          string        `json:"key" toml:"key" yaml:"key"`
	WaitDuration time.Duration `json:"wait_duration" toml:"wait_duration" yaml:"wait_duration"`

	DelayKey          string        `json:"delay_key" toml:"delay_key" yaml:"delay_key"`
	DelayWaitDuration time.Duration `json:"delay_wait_duration" yaml:"delay_wait_duration" json:"delay_wait_duration"`

	Concurrent int             `json:"concurrent" toml:"concurrent" yaml:"concurrent"`  // 并行数量
	RetryRule  []time.Duration `json:"retry_rule" toml:"retry_rule"  yaml:"retry_rule"` // 重试规则
}

type Rmq struct {
	// 配置信息
	config *Config

	// 队列退出信号
	concurrentChan     chan int
	exitQueueChan      chan int
	exitDelayQueueChan chan int

	log Logger

	// 关闭方法
	exitFunc context.CancelFunc

	// 执行引擎
	Broker Broker
	Process

	// 钩子
	Hook func()
}

// NewRmq 创建新队列
func NewRmq(redis *redis.Client, c *Config) *Rmq {
	rmq := &Rmq{
		config:             c,
		concurrentChan:     make(chan int, c.Concurrent),
		exitQueueChan:      make(chan int),
		exitDelayQueueChan: make(chan int),
		Process:            NewDefaultProcess(),
	}

	rmq.Register("simpleTask", &SimpleTask{})
	rmq.Register("commandTask", &CommandTask{})
	rmq.Register("httpTask", &HttpTask{})
	return rmq
}

// SetProcess 自定义处理引擎
func (q *Rmq) SetProcess(p Process) {
	q.Process = p
}

// SetBroker 自定义处理引擎
func (q *Rmq) SetBroker(b Broker) {
	q.Broker = b
}

// Start 线上是多容器的，不用多个协程并发跑,只要加pod就行
// 某前理论存在丢失消息的可能，所以只能用于不重要的任务
func (q *Rmq) Start(ctx context.Context) {
	ctx, q.exitFunc = context.WithCancel(ctx)
	q.Broker.Start(ctx)
}

// Exit 退出
func (q *Rmq) Exit() {
	q.log.Infof("rmq 开始退出")
	q.exitFunc()
	q.log.Infof("rmq 退出成功")
}

// Push 写入消息到队列
func (q *Rmq) Push(ctx context.Context, msg ...*Message) (v int64, err error) {
	v, err = q.Broker.Push(ctx, msg...)
	return
}

func (q *Rmq) addConcurrent() {
	q.concurrentChan <- 1
}

func (q *Rmq) doneConcurrent() {
	<-q.concurrentChan
}

// TryRun 解析消息，执行
func (q *Rmq) TryRun(ctx context.Context, data string) {
	var msg *Message
	// 解析,这一步失败了就不要往队列扔了，要处理的话，应该扔进死信队列,这个时候消息会丢失
	if err := json.Unmarshal([]byte(data), &msg); err != nil || msg == nil {
		q.log.Errorf("解析队列数据异常：%s,data: %s", err, data)
		return
	}

	// 超时控制
	var cancelFunc context.CancelFunc
	if msg.Meta.Timeout == 0 {
		msg.Meta.Timeout = 30
	}

	ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(msg.Meta.Timeout)*time.Second)
	go func() {
		var err error
		defer cancelFunc()
		defer func() {
			// 查看有无panic
			if errX := recover(); errX != nil {
				err = fmt.Errorf("任务%s panic: %v", msg.Id, errX)
				q.log.Errorf("任务%s panic: %v，将到下一个时间点重试", msg.Id, err)
			}
			if err == nil {
				return
			}

			// 有错误，就重试
			if err = q.TryRetry(ctx, msg); err != nil {
				q.log.Errorf("任务%s重试失败,%s", msg.Id, err)
			} else {
				q.log.Infof("任务%重试成功,将在%s开始重试", msg.Id, msg.RunAt.DateTime())
			}
		}()

		// 执行
		var result string
		if result, err = q.Process.Run(ctx, msg); err != nil {
			q.log.Errorf("任务%s执行失败,%s", msg.Id, err)
			return
		}
		q.log.Infof("任务%s执行成功,Result %s", msg.Id, result)
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

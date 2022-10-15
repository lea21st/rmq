package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
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

type Queue struct {
	redis *redis.Client

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
	process Process

	// 钩子
	Hook func()
}

// NewQueue 创建新队列
func NewQueue(redis *redis.Client, c *Config) *Queue {
	return &Queue{
		redis:              redis,
		config:             c,
		concurrentChan:     make(chan int, c.Concurrent),
		exitQueueChan:      make(chan int),
		exitDelayQueueChan: make(chan int),
		process:            NewDefaultProcess(),
	}
}

// SetProcess 自定义处理引擎
func (q *Queue) SetProcess(p Process) {
	q.process = p
}

// Start 线上是多容器的，不用多个协程并发跑,只要加pod就行
// 某前理论存在丢失消息的可能，所以只能用于不重要的任务
func (q *Queue) Start(ctx context.Context) {
	ctx, q.exitFunc = context.WithCancel(ctx)
	// 开始队列，并自动重启
	go q.DaemonStart(ctx, q.startReceiveQueue)
	go q.DaemonStart(ctx, q.startReceiveDelayQueue)
}

func (q *Queue) DaemonStart(ctx context.Context, action func(ctx context.Context)) {
	rebootDuration := time.Minute
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			log.Printf("队列异常:%s,将在%s后重启", err, rebootDuration)
			// 等一会重启
			time.AfterFunc(rebootDuration, func() {
				action(ctx)
			})
		}
	}()
	action(ctx)
}

// Exit 退出
func (q *Queue) Exit() {
	q.log.Infof("rmq 开始退出")
	q.exitFunc()
	q.log.Infof("rmq 退出成功")
}

// PushAll 批量写入消息
func (q *Queue) PushAll(ctx context.Context, msg ...Message) (v int64) {
	var delayMessages []*redis.Z
	var messages []interface{}
	for _, v := range msg {
		if v.Meta.Delay > 0 {
			delayMessages = append(delayMessages, &redis.Z{
				Score:  float64(v.RunAt),
				Member: v.String(),
			})
		} else {
			messages = append(messages, v.String())
		}
	}
	if len(delayMessages) > 0 {
		v = q.redis.ZAdd(ctx, q.config.DelayKey, delayMessages...).Val()
	}
	if len(messages) > 0 {
		v += q.redis.RPush(ctx, q.config.DelayKey, messages...).Val()
	}
	return
}

// Push 写入消息到队列
func (q *Queue) Push(ctx context.Context, msg Message) (err error) {
	if msg.Meta.Delay > 0 {
		_, err = q.redis.ZAdd(ctx, q.config.DelayKey, &redis.Z{
			Score:  float64(msg.RunAt),
			Member: msg.String(),
		}).Result()
	}
	if msg.Meta.Delay == 0 {
		_, err = q.redis.RPush(ctx, q.config.DelayKey, msg.String()).Result()
	}
	return
}

func (q *Queue) addConcurrent() {
	q.concurrentChan <- 1
}

func (q *Queue) doneConcurrent() {
	<-q.concurrentChan
}

// 延时队列消息处理
func (q *Queue) startReceiveDelayQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			var members []redis.Z
			ctx := context.TODO()
			if members, err = q.redis.ZRangeByScoreWithScores(ctx, q.config.DelayKey, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.Itoa(int(Now())),
				Offset: 0,
				Count:  10,
			}).Result(); err != nil || len(members) == 0 {
				if err != nil {
					q.log.Errorf("获取队列(%s)数据异常:%s", q.config.DelayKey, err)
				}
				time.Sleep(q.config.DelayWaitDuration)
				continue
			}

			// 并行执行
			for _, v := range members {
				// 不要使用协程
				func() {
					q.addConcurrent()
					defer q.doneConcurrent()
					if q.redis.ZRem(ctx, q.config.DelayKey, v.Member).Val() > 0 {
						q.TryRun(context.Background(), v.Member.(string))
					}
				}()
			}
		}
	}
}

// 普通队列消息处理
func (q *Queue) startReceiveQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ctx := context.TODO()
			// 由于一些线上禁用了BLPop命令,就用LPop
			data, err := q.redis.LPop(ctx, q.config.Key).Result()
			if err == redis.Nil {
				time.Sleep(q.config.WaitDuration)
				continue
			}

			if err != nil {
				q.log.Errorf("获取队列(%s)数据异常:%s，data:%s", q.config.Key, err, data)
				time.Sleep(q.config.WaitDuration)
				continue
			}
			// 不要使用协程
			func() {
				q.addConcurrent()
				defer q.doneConcurrent()
				q.TryRun(ctx, data)
			}()

		}
	}
}

// TryRun 解析消息，执行
func (q *Queue) TryRun(ctx context.Context, data string) {
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
		if err = q.process.Run(ctx, msg); err != nil {
			q.log.Errorf("任务%s执行失败,%s", msg.Id, err)
			return
		}
		q.log.Infof("任务%s执行成功", msg.Id)
		return
	}()
	<-ctx.Done()
}

// TryRetry 尝试重试
func (q *Queue) TryRetry(ctx context.Context, msg *Message) (err error) {
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
	q.Push(ctx, msg)
	return
}

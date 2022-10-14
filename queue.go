package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

type Config struct {
	Key          string
	WaitDuration time.Duration

	DelayKey          string
	DelayWaitDuration time.Duration

	Concurrent int
	RetryRule  []time.Duration // 重试规则
}

type HookFunc func(ctx context.Context, msg *Message, resp []byte, err error)

type Queue struct {
	redis *redis.Client

	// 配置信息
	config *Config

	// 队列退出信号
	concurrentChan     chan int
	exitQueueChan      chan int
	exitDelayQueueChan chan int

	log Logger

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
	}
}

func (q *Queue) addConcurrent() {
	q.concurrentChan <- 1
}

func (q *Queue) doneConcurrent() {
	<-q.concurrentChan
}

// Start 线上是多容器的，不用多个协程并发跑,只要加pod就行
// 某前理论存在丢失消息的可能，所以只能用于不重要的任务
func (q *Queue) Start() {
	// 开始队列，并自动重启
	rebootDuration := 1 * time.Minute
	go DaemonCoroutine("消息队列", rebootDuration, q.redi)
	go DaemonCoroutine("延时队列", rebootDuration, q.startReceiveDelayQueue)
}

// Exit 退出
func (q *Queue) Exit() {
	q.exitQueueChan <- 1
	q.exitDelayQueueChan <- 1
}

func (q *Queue) exitListen() {

}
func (q *Queue) startReceiveDelayQueue() {
	for {
		select {
		case <-q.exitDelayQueueChan:
			break
		default:
			{
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
					q.addConcurrent()
					go func(v redis.Z) {
						defer q.doneConcurrent()
						if q.redis.ZRem(ctx, q.delayKey, v.Member).Val() == 0 {
							return
						}
						q.TryRun(v.Member.(string))
					}(v)
				}
			}
		}
	}
}

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
			q.addConcurrent()
			go func(data string) {
				defer q.doneConcurrent()
				q.TryRun(data)
			}(data)
		}
	}
}

// TryRun 解析消息，执行
func (q *Queue) TryRun(ctx context.Context, data string) {
	var (
		cancel context.CancelFunc
		msg    *Message
		resp   []byte
		err    error
	)

	// 解析,这一步失败了就不要往队列扔了，要处理的话，应该扔进死信队列
	if err = json.Unmarshal([]byte(data), &msg); err != nil || msg == nil {
		q.log.Errorf("解析队列数据异常：%s,(%data)", err, data)
		return
	}

	// 防止panic
	defer func() {
		if err := recover(); err != nil {
			q.log.Errorf("任务%s panic(%s)，将到下一个时间点重试", msg.Id, err)
			_ = q.TryRetry(ctx, msg)
		}
	}()

	var resp []byte
	if resp, err = msg.Exec(ctx); err != nil {
		return
	}

	// 完成钩子
	defer func() {
		for i := range q.CompleteHook {
			q.CompleteHook[i](ctx, msg, resp, err)
		}
	}()

	// 防止超时
	_, cancel = context.WithTimeout(ctx, time.Duration(msg.Timeout)*time.Second+time.Second)
	defer cancel()

	if msg.ExpiredAt <= Now() {
		q.log.Errorf("任务%s执行失败,错误原因：任务于%s过期", msg.Id, msg.ExpiredAt.DateTime())
		return
	}

	if resp, err = msg.Exec(ctx); err != nil {
		q.log.Errorf("任务%s执行失败,%s", msg.Id, err)
		// 是否需要重试
		if err := q.TryRetry(ctx, msg); err != nil {
			q.log.Errorf("任务%s重试失败,%s", msg.Id, err)
		} else {
			q.log.Errorf("任务%s将在%s开始重试", msg.Id, msg.RunAt.DateTime())
		}
		return
	}
	q.log.Infof("任务%s执行成功", msg.Id)
	return
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
	// q.Push(ctx, msg) todo
	return
}

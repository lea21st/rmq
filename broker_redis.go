package rmq

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisBrokerConfig struct {
	Key          string        `json:"key" toml:"key" yaml:"key"`
	WaitDuration time.Duration `json:"wait_duration" toml:"wait_duration" yaml:"wait_duration"`

	DelayKey          string        `json:"delay_key" toml:"delay_key" yaml:"delay_key"`
	DelayWaitDuration time.Duration `json:"delay_wait_duration" yaml:"delay_wait_duration" json:"delay_wait_duration"`
}

type RedisBroker struct {
	redis  *redis.Client
	config RedisBrokerConfig
	log    Logger
	ctx    context.Context
	msg    chan<- *Message
}

func (r *RedisBroker) Encode(msg *Message) ([]byte, error) {
	return json.Marshal(msg)
}

func (r *RedisBroker) Decode(bytes []byte, msg *Message) error {
	return json.Unmarshal(bytes, msg)
}

// Push 批量写入消息
func (r *RedisBroker) Push(ctx context.Context, msg ...Message) (v int64, err error) {
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
		v1, err1 := r.redis.ZAdd(ctx, r.config.DelayKey, delayMessages...).Result()
		v += v1
		if err1 != nil {
			err = err1
		}
	}
	if len(messages) > 0 {
		v2, err2 := r.redis.RPush(ctx, r.config.DelayKey, messages...).Result()
		v += v2
		if err2 != nil {
			err = err2
		}
	}
	return
}

func (r *RedisBroker) Start(ctx context.Context, ch chan<- *Message) {
	rebootDuration := time.Minute
	DaemonStart(rebootDuration, r.Consumer)
	DaemonStart(rebootDuration, r.consumerDelayQueue)
}

// 延时队列消息处理
func (r *RedisBroker) consumerDelayQueue() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			var err error
			var members []redis.Z
			ctx := context.TODO()
			if members, err = r.redis.ZRangeByScoreWithScores(ctx, r.config.DelayKey, &redis.ZRangeBy{
				Min:    "0",
				Max:    strconv.Itoa(int(Now())),
				Offset: 0,
				Count:  10,
			}).Result(); err != nil || len(members) == 0 {
				if err != nil {
					r.log.Errorf("获取队列(%s)数据异常:%s", r.config.DelayKey, err)
				}
				time.Sleep(r.config.DelayWaitDuration)
				continue
			}

			// 并行执行
			for _, v := range members {
				// 不要使用协程
				if r.redis.ZRem(ctx, r.config.DelayKey, v.Member).Val() > 0 {
					if _, err = r.redis.RPush(ctx, r.config.Key, v.Member.(string)).Result(); err != nil {
						r.log.Errorf("队列数据写入失败:%s,Data: %s", err, v.Member)
					}
				}
			}
		}
	}
}

// Consumer 普通队列消息处理
func (r *RedisBroker) Consumer() {
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			ctx := context.TODO()
			// 由于一些线上禁用了BLPop命令,就用LPop
			data, err := r.redis.LPop(ctx, r.config.Key).Result()
			if err == redis.Nil {
				time.Sleep(r.config.WaitDuration)
				continue
			}

			if err != nil {
				r.log.Errorf("获取队列(%s)数据异常:%s，data:%s", r.config.Key, err, data)
				time.Sleep(r.config.WaitDuration)
				continue
			}

			// 不要使用协程
			var msg Message
			if err = r.Decode([]byte(data), &msg); err != nil {
				r.log.Errorf("消息%s解析失败:%s", data, err)
				continue
			}
			r.msg <- &msg
		}
	}
}

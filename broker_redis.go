package rmq

import (
	"context"
	"encoding/json"
	"fmt"
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

var DefaultRedisBrokerConfig = RedisBrokerConfig{
	Key:               "rmq:queue",
	WaitDuration:      1 * time.Second,
	DelayKey:          "rmq:queue:delay",
	DelayWaitDuration: 4 * time.Second,
}

type RedisBroker struct {
	redis    *redis.Client
	config   RedisBrokerConfig
	log      Logger
	exitChan chan int
}

func NewRedisBroker(rd *redis.Client, c RedisBrokerConfig, log Logger) *RedisBroker {
	return &RedisBroker{
		redis:    rd,
		config:   c,
		log:      log,
		exitChan: make(chan int),
	}
}

func (r *RedisBroker) Encode(msg *Message) ([]byte, error) {
	return json.Marshal(msg)
}

func (r *RedisBroker) Decode(bytes []byte) (msg *Message, err error) {
	err = json.Unmarshal(bytes, &msg)
	return
}

// Push 批量写入消息
func (r *RedisBroker) Push(ctx context.Context, msg ...*Message) (v int64, err error) {
	var delayMessages []*redis.Z
	var messages []interface{}
	for i, v := range msg {
		data, _ := r.Encode(msg[i])
		if v.Meta.Delay > 0 {
			delayMessages = append(delayMessages, &redis.Z{
				Score:  float64(v.RunAt),
				Member: string(data),
			})
		} else {
			messages = append(messages, string(data))
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
		v2, err2 := r.redis.RPush(ctx, r.config.Key, messages...).Result()
		v += v2
		if err2 != nil {
			err = err2
		}
	}
	return
}

// Pop 获取message
func (r *RedisBroker) Pop(ctx context.Context) (msg *Message, err error) {
	// 由于一些线上禁用了BLPop命令,就用LPop
	var data string
	if data, err = r.redis.LPop(ctx, r.config.Key).Result(); err != nil {
		if err == redis.Nil {
			err = nil
		}
		return
	}

	if msg, err = r.Decode([]byte(data)); err != nil {
		err = fmt.Errorf("消息%s解析失败:%s", data, err)
		return
	}
	return
}

func (r *RedisBroker) BeforeStart() {
	// todo
}

func (r *RedisBroker) AfterStart() {
	// 将异步队列的数据，写入到队列
	for {
		select {
		case <-r.exitChan:
			r.log.Infof("Redis broker exit")
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
				if r.redis.ZRem(ctx, r.config.DelayKey, v.Member).Val() > 0 {
					if _, err = r.redis.RPush(ctx, r.config.Key, v.Member.(string)).Result(); err != nil {
						r.log.Errorf("队列数据写入失败:%s,Data: %s", err, v.Member)
					}
				}
			}
		}
	}
}

func (r *RedisBroker) BeforeExit() {
	// todo
}

func (r *RedisBroker) AfterExit() {
	// todo
}

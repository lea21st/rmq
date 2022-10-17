package rmq

import (
	"context"
	"encoding/json"
	"time"

	uuid "github.com/satori/go.uuid"
)

type Messages []*Message

type Message struct {
	Id        string          `json:"id"`
	Task      string          `json:"task"`
	Data      json.RawMessage `json:"data"`
	RunAt     Millisecond     `json:"run_at"`     // 应执行时间
	ExpiredAt Millisecond     `json:"expired_at"` // 过期时间
	CreatedAt Millisecond     `json:"created_at"` // 创建时间
	Meta      Meta            `json:"meta,omitempty"`
}

func NewMsg() *Message {
	now := NowSecond()
	msg := &Message{
		Id:        uuid.NewV4().String(),
		RunAt:     now,
		ExpiredAt: now.Add(24 * time.Hour), // 默认24小时过期
		Meta:      RetryMeta,
		CreatedAt: Now(),
	}
	return msg
}
func NewDefaultMsg(name string, data any) *Message {
	now := NowSecond()
	msg := &Message{
		Id:        uuid.NewV4().String(),
		Task:      name,
		RunAt:     now,
		ExpiredAt: now.Add(24 * time.Hour), // 默认24小时过期
		CreatedAt: Now(),
	}
	msg.SetData(data)
	msg.SetMeta(DefaultMeta)
	return msg
}
func NewBlankMsg() *Message {
	now := NowSecond()
	msg := &Message{
		Id:        uuid.NewV4().String(),
		RunAt:     now,
		ExpiredAt: now.Add(24 * time.Hour), // 默认24小时过期
		Meta:      DefaultMeta,
		CreatedAt: Now(),
	}
	return msg
}

func NewMsgWithTask(task Task) *Message {
	now := NowSecond()
	msg := &Message{
		Id:        uuid.NewV4().String(),
		Task:      "",
		RunAt:     now,
		ExpiredAt: now.Add(24 * time.Hour), // 默认24小时过期
		Meta:      RetryMeta,
		CreatedAt: Now(),
	}
	return msg
}

func (m *Message) SetTask(task Task) *Message {
	return m
}

func (m *Message) SetCallback(callback Callback, data any) *Message {
	m.Data, _ = json.Marshal(data)
	return m
}

func (m *Message) SetMeta(meta Meta) *Message {
	m.Meta = meta
	return m
}

func (m *Message) SetData(data any) *Message {
	m.Data, _ = json.Marshal(data)
	return m
}
func (m *Message) SetRawData(data json.RawMessage) *Message {
	m.Data = data
	return m
}

func (m *Message) SetMaxRetry(retry int) *Message {
	m.Meta.Retry[1] = retry
	return m
}

func (m *Message) SetDelay(delay time.Duration) *Message {
	m.Meta.Delay = int(delay.Seconds())
	m.RunAt = m.RunAt.Add(delay)
	return m
}

func (m *Message) SetExpiredAt(t time.Time) *Message {
	m.ExpiredAt = CreateMillisecond(t)
	return m
}

func (m *Message) SetExpire(d time.Duration) *Message {
	m.ExpiredAt = m.RunAt.Add(d)
	return m
}

func (m *Message) SetTimeout(t time.Duration) *Message {
	m.Meta.Timeout = int(t.Seconds())
	return m
}

func (m *Message) SetCheckRule(rule map[string]any) *Message {
	m.Meta.CheckRule = rule
	return m
}

func (m *Message) SetTraceId(traceId string) *Message {
	m.Meta.TraceId = traceId
	return m
}

func (m *Message) TryRetry(delay time.Duration) *Message {
	m.Meta.Delay = int(delay.Seconds())
	m.RunAt = NowSecond().Add(delay)
	return m
}

func (m *Message) String() string {
	data, _ := json.Marshal(m)
	return string(data)
}

func (m *Message) Push(ctx context.Context, q *Rmq) (err error) {
	_, err = q.Push(ctx, m)
	return
}

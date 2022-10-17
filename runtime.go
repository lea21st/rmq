package rmq

import "time"

type TaskRuntime struct {
	Msg       *Message
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	RunErr    error // 执行错误
	Error     error // 最后的错误
	Result    any   // 结果
}

func NewTaskRuntime(msg *Message) *TaskRuntime {
	return &TaskRuntime{Msg: msg}
}

func (a *TaskRuntime) IsSuccess() bool {
	return a.RunErr == nil
}

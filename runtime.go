package rmq

import (
	"time"
)

type TaskRuntime struct {
	Msg       *Message      `json:"msg"`
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	Duration  time.Duration `json:"duration"`
	TaskError error         `json:"task_error,omitempty"` // 执行错误
	Error     error         `json:"error,omitempty"`      // 最后的错误
	Result    string        `json:"result,omitempty"`     // 结果
}

func newTaskRuntime(msg *Message) *TaskRuntime {
	return &TaskRuntime{Msg: msg}
}

func (a *TaskRuntime) IsSuccess() bool {
	return a.TaskError == nil
}

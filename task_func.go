package rmq

import (
	"context"
	"fmt"
)

type FuncTask struct {
	msg      *Message
	callback Callback
	taskName string
}

func NewFuncTask(name string, callback Callback) *FuncTask {
	return &FuncTask{
		callback: callback,
		taskName: name,
	}
}
func (s *FuncTask) TaskName() string {
	return s.taskName
}

func (s *FuncTask) Load(ctx context.Context, msg *Message) error {
	s.msg = msg
	return nil
}

func (s *FuncTask) Run(ctx context.Context) (any, error) {
	if s.callback == nil {
		fmt.Println("callb", s.msg)
	}
	return s.callback(ctx, s.msg)
}

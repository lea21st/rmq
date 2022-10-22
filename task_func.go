package rmq

import (
	"context"
	"fmt"
)

type callbackTask struct {
	msg      *Message
	callback Callback
	taskName string
}

func newFuncTask(name string, callback Callback) *callbackTask {
	return &callbackTask{
		callback: callback,
		taskName: name,
	}
}
func (s *callbackTask) TaskName() string {
	return s.taskName
}

func (s *callbackTask) Load(ctx context.Context, msg *Message) error {
	s.msg = msg
	return nil
}

func (s *callbackTask) Run(ctx context.Context) (string, error) {
	if s.callback == nil {
		return "", fmt.Errorf("callback is null")
	}
	return s.callback(ctx, s.msg)
}

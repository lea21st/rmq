package rmq

import (
	"context"
)

type SimpleTask struct {
	msg      *Message
	callback Callback
}

func (s *SimpleTask) Run(ctx context.Context) (string, error) {
	return s.callback(ctx, s.msg)
}

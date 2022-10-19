package rmq

import "context"

type Hook struct {
	onPush     func(ctx context.Context, msg ...*Message) ([]*Message, error)
	onContext  func(ctx context.Context) context.Context
	onRun      func(ctx context.Context, runtime *TaskRuntime) error
	onComplete func(ctx context.Context, runtime *TaskRuntime) error
}

func (h *Hook) OnPush(v func(ctx context.Context, msg ...*Message) ([]*Message, error)) {
	h.onPush = v
}

func (h *Hook) OnContext(v func(ctx context.Context) context.Context) {
	h.onContext = v
}

func (h *Hook) OnRun(v func(ctx context.Context, runtime *TaskRuntime) error) {
	h.onRun = v
}

func (h *Hook) OnComplete(v func(ctx context.Context, runtime *TaskRuntime) error) {
	h.onComplete = v
}

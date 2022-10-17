package rmq

import "context"

type HookFunc func(ctx context.Context, runtime *TaskRuntime)
type Hook struct {
	onPush     func(ctx context.Context, msg ...*Message) ([]*Message, error)
	onRun      HookFunc
	onRetry    HookFunc
	onComplete HookFunc
}

func (h *Hook) OnPush(v func(ctx context.Context, msg ...*Message) ([]*Message, error)) {
	h.onPush = v
}

func (h *Hook) OnRun(v HookFunc) {
	h.onRun = v
}

func (h *Hook) OnRetry(v HookFunc) {
	h.onRetry = v
}

func (h *Hook) OnComplete(v HookFunc) {
	h.onComplete = v
}

func (h *Hook) Protect() {

}

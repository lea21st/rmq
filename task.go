package rmq

import "context"

// Task 自动实例化的Task
type Task interface {
	Run(ctx context.Context) error
}

// OnLoad 加载时执行的方法
type OnLoad interface {
	OnLoad(ctx context.Context, msg *Message) error
}

// OnSuccess 执行成功时执行的方法
type OnSuccess interface {
	OnSuccess(ctx context.Context)
}

// OnFail 执行失败时执行的方法
type OnFail interface {
	OnFail(ctx context.Context)
}

// OnComplete 执行完成时会调用
type OnComplete interface {
	OnComplete(ctx context.Context)
}

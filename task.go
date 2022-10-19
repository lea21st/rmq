package rmq

import "context"

type Callback func(ctx context.Context, msg *Message) (string, error)

// Task 自动实例化的Task
type Task interface {
	TaskName() string
	Run(ctx context.Context) (result any, err error)
}
type TaskName interface {
	TaskName() string
}

type TaskScanner interface {
	Scan(src []byte) error
}

type TaskValuer interface {
	Value() ([]byte, error)
}

// OnLoad 加载时执行的方法
type OnLoad interface {
	Load(ctx context.Context, msg *Message) error
}

// OnSuccess 执行成功时执行的方法
type OnSuccess interface {
	Success(ctx context.Context)
}

// OnFail 执行失败时执行的方法
type OnFail interface {
	Fail(ctx context.Context)
}

// OnComplete 执行完成时会调用
type OnComplete interface {
	Complete(ctx context.Context)
}

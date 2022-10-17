package rmq

import (
	"context"
	"reflect"
)

type TaskInfo struct {
	Name        string
	Path        string
	IsFunc      bool
	ReflectType reflect.Type
}

type Process interface {
	Register(name string, task Task) // 手动定义名称，因为对于历史消息，方法名都有可能时改的
	RegisterFunc(name string, callback Callback)
	GetTaskInfo(name string) *TaskInfo
	Exec(ctx context.Context, msg *TaskRuntime) (result string, err error)
}

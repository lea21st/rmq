package rmq

import (
	"context"
	"reflect"
)

type TaskInfo struct {
	Name        string
	IsCallback  bool
	ReflectType reflect.Type
	Callback    Callback
}

type Process interface {
	Exec(ctx context.Context, msg *TaskRuntime) (err error)
}

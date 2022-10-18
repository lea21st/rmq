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
	Exec(ctx context.Context, msg *TaskRuntime) (result string, err error)
}

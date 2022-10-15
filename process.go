package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type Process interface {
	Register(name string, task any)
	Run(ctx context.Context, msg *Message) (err error)
}
type DefaultProcess struct {
	Tasks    map[string]reflect.Type
	taskLock sync.RWMutex
}

func NewDefaultProcess() *DefaultProcess {
	return &DefaultProcess{
		Tasks:    make(map[string]reflect.Type),
		taskLock: sync.RWMutex{},
	}
}

// Register 注册方法
func (p *DefaultProcess) Register(name string, task any) {
	p.taskLock.Lock()
	defer p.taskLock.Unlock()
	if task == nil {
		panic("task不能为nil")
	}
	t := reflect.TypeOf(task)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	p.Tasks[name] = t
}

func (p *DefaultProcess) Run(ctx context.Context, msg *Message) (err error) {
	rType, ok := p.Tasks[msg.Task]
	if !ok {
		err = fmt.Errorf("%s未注册", msg.Task)
		return
	}
	val := reflect.New(rType).Interface()

	// 实例化数据
	if impl, ok := val.(OnLoad); ok {
		if err = impl.OnLoad(ctx, msg); err != nil {
			return
		}
	} else {
		if err = json.Unmarshal(msg.Data, &val); err != nil {
			return
		}
	}

	// 执行
	var task Task
	if task, ok := val.(Task); !ok {
		err = fmt.Errorf("%s不是一个有效的task", msg.Task)
		return
	} else {
		err = task.Run(ctx)
	}

	// 执行成功事件
	if impl, ok := task.(OnSuccess); ok && err == nil {
		impl.OnSuccess(ctx)
	}
	// 执行失败事件
	if impl, ok := task.(OnFail); ok && err == nil {
		impl.OnFail(ctx)
	}
	// 执行完成事件
	if impl, ok := task.(OnComplete); ok && err == nil {
		impl.OnComplete(ctx)
	}
	return
}

package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

type DefaultProcess struct {
	Tasks    map[string]TaskInfo
	taskLock sync.RWMutex
}

func NewDefaultProcess() *DefaultProcess {
	return &DefaultProcess{
		Tasks:    make(map[string]TaskInfo),
		taskLock: sync.RWMutex{},
	}
}

func (p *DefaultProcess) Register(name string, task Task) {
	p.taskLock.Lock()
	defer p.taskLock.Unlock()

	t := reflect.TypeOf(task)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	p.Tasks[name] = TaskInfo{
		Name:        name,
		Path:        fmt.Sprintf("%s.%s", t.PkgPath(), t.Name()),
		ReflectType: t,
	}
}

func (p *DefaultProcess) RegisterFunc(name string, callback Callback) {
	p.taskLock.Lock()
	defer p.taskLock.Unlock()
	task := &SimpleTask{callback: callback}
	t := reflect.TypeOf(task)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	p.Tasks[name] = TaskInfo{
		Name:        name,
		IsFunc:      true,
		Path:        runtime.FuncForPC(reflect.ValueOf(callback).Pointer()).Name(),
		ReflectType: t,
	}
}
func (p *DefaultProcess) GetTaskInfo(name string) TaskInfo {
	return p.Tasks[name]
}

func (p *DefaultProcess) Run(ctx context.Context, msg *Message) (result string, err error) {
	info, ok := p.Tasks[msg.Task]
	if !ok || info.ReflectType == nil {
		err = fmt.Errorf("%s未注册", msg.Task)
		return
	}
	val := reflect.New(info.ReflectType).Interface()

	if !info.IsFunc {
		// 实例化数据,如果没有实现TaskScanner，使用json尝试
		if impl, ok := val.(TaskScanner); ok {
			err = impl.Scan(msg.Data)
		} else {
			err = json.Unmarshal(msg.Data, &val)
		}
		if err != nil {
			err = fmt.Errorf("数据解析失败：%s", err)
			return
		}
	}

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
		result, err = task.Run(ctx)
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

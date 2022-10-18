package rmq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

type DefaultProcess struct {
	Tasks    map[string]*TaskInfo
	taskLock sync.RWMutex
}

func NewDefaultProcess() *DefaultProcess {
	return &DefaultProcess{
		Tasks:    make(map[string]*TaskInfo),
		taskLock: sync.RWMutex{},
	}
}

func (p *DefaultProcess) Exec(ctx context.Context, run *TaskRuntime) (result string, err error) {
	msg := run.Msg
	if msg == nil {
		err = fmt.Errorf("无法识别的任务")
		return
	}

	info, ok := p.Tasks[msg.Task]
	if !ok || info == nil || info.ReflectType == nil {
		err = fmt.Errorf("任务%s未注册", msg.Task)
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
		if err = impl.Load(ctx, msg); err != nil {
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
		run.Result, err = task.Run(ctx)
	}

	// 执行成功事件
	if impl, ok := task.(OnSuccess); ok && err == nil {
		impl.Success(ctx)
	}
	// 执行失败事件
	if impl, ok := task.(OnFail); ok && err == nil {
		impl.Fail(ctx)
	}
	// 执行完成事件
	if impl, ok := task.(OnComplete); ok && err == nil {
		impl.Complete(ctx)
	}
	return
}

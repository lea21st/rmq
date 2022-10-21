package rmq

import (
	"context"
	"encoding/json"
	"fmt"
)

type defaultProcess struct {
	*Register
	log Logger
}

func newDefaultProcess(log Logger) *defaultProcess {
	return &defaultProcess{Register: register, log: log}
}

func (p *defaultProcess) Exec(ctx context.Context, run *TaskRuntime) (err error) {
	msg := run.Msg
	if msg == nil {
		err = fmt.Errorf("null message")
		return
	}

	var task Task
	var taskInfo *TaskInfo
	if task, taskInfo, err = p.CreateTask(run.Msg.Task); err != nil {
		return
	}

	// 实例化数据,如果没有实现TaskScanner，使用json尝试
	if !taskInfo.IsCallback {
		if impl, ok := task.(TaskScanner); ok {
			if err = impl.Scan(msg.Data); err != nil {
				err = fmt.Errorf("failed to decode task message: %s", err)
				return
			}
		} else {
			if err = json.Unmarshal(run.Msg.Data, task); err != nil {
				err = fmt.Errorf("failed to unmarshal message: %s", err)
				return
			}
		}
	}

	if impl, ok := task.(OnLoad); ok {
		if err = impl.Load(ctx, msg); err != nil {
			err = fmt.Errorf("task load error: %s", err)
			return
		}
	}

	// 执行
	if run.Result, err = task.Run(ctx); err != nil {
		err = fmt.Errorf("failed to run %s: %s", task.TaskName(), err)
	}

	// 执行成功事件
	if impl, ok := task.(OnSuccess); ok && err == nil {
		if errX := Protect(func() error {
			impl.OnSuccess(ctx)
			return nil
		}); errX != nil {
			p.log.Errorf("failed to run %s.OnSuccess, Id:%s, Error:%s", msg.Task, msg.Id, errX)
		}
	}
	// 执行失败事件
	if impl, ok := task.(OnFail); ok && err != nil {
		if errX := Protect(func() error {
			impl.OnFail(ctx)
			return nil
		}); errX != nil {
			p.log.Errorf("failed to run %s.OnFail, Id:%s, Error:%s", msg.Task, msg.Id, errX)
		}
	}
	// 执行完成事件
	if impl, ok := task.(OnComplete); ok {
		if errX := Protect(func() error {
			impl.OnComplete(ctx)
			return nil
		}); errX != nil {
			p.log.Errorf("failed to run %s.OnComplete, Id:%s, Error:%s", msg.Task, msg.Id, errX)
		}
	}
	return
}

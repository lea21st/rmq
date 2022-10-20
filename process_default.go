package rmq

import (
	"context"
	"encoding/json"
	"fmt"
)

type DefaultProcess struct {
	*Register
}

func NewDefaultProcess() *DefaultProcess {
	return &DefaultProcess{Register: register}
}

func (p *DefaultProcess) Exec(ctx context.Context, run *TaskRuntime) (err error) {
	msg := run.Msg
	if msg == nil {
		err = fmt.Errorf("无法识别的任务")
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
				err = fmt.Errorf("scan error: %s", err)
				return
			}
		} else {
			if err = json.Unmarshal(run.Msg.Data, task); err != nil {
				err = fmt.Errorf("scan error: %s", err)
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
		err = fmt.Errorf("task run error: %s", err)
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

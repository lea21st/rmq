package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lea21st/rmq"
)

func Test1(ctx context.Context, msg *rmq.Message) (result string, err error) {
	data, _ := json.Marshal(msg)
	result = string(data)
	fmt.Println("执行了任务1", result, err)
	return
}

func Test2(ctx context.Context, msg *rmq.Message) (result string, err error) {
	data, _ := json.Marshal(msg)
	result = string(data)
	fmt.Println("执行了任务2", result, err)
	return
}

type TestTask struct {
	Name string `json:"name"`
	Val  int    `json:"val"`
}

func (t *TestTask) OnSuccess(ctx context.Context) {
	fmt.Println(t.Name, "success hook")
	panic("OnSuccess")
}

func (t *TestTask) OnFail(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Fail hook")
	panic("panic OnFail")
}

func (t *TestTask) OnComplete(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Complete hook")
	panic("OnComplete")
}

func (t *TestTask) TaskName() string {
	// TODO implement me
	return "TestTask"
}

func (t *TestTask) Run(ctx context.Context) (result string, err error) {
	return "ok", nil
}

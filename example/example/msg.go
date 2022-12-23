package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lpong/rmq"
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
}

func (t *TestTask) OnFail(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Fail hook")
}

func (t *TestTask) OnComplete(ctx context.Context) {
	// TODO implement me
	fmt.Println(t.Name, "Complete hook")
}

func (t *TestTask) TaskName() string {
	// TODO implement me
	return "TestTask"
}

func (t *TestTask) Run(ctx context.Context) (result string, err error) {
	return "fail", fmt.Errorf("fail")
}

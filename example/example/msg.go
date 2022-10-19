package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

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

func (t *TestTask) Value() ([]byte, error) {
	return json.Marshal(t)
}

func (t *TestTask) Scan(src []byte) error {
	return json.Unmarshal(src, &t)
}

func (t *TestTask) TaskName() string {
	// TODO implement me
	return "TestTask"
}

func (t *TestTask) Run(ctx context.Context) (result any, err error) {
	time.Sleep(1 * time.Second)
	rand.Seed(time.Now().UnixNano())
	if rand.Int31n(10) > 5 {
		return "fail", fmt.Errorf("随机错误")
	}
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Int63n(60)) * time.Second)
	return "ok", nil
}

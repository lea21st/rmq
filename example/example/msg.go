package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lea21st/rmq"
)

func Test(ctx context.Context, msg *rmq.Message) (result string, err error) {
	data, _ := json.Marshal(msg)
	result = string(data)
	fmt.Println("执行了任务", result, err)
	return
}

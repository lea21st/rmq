package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/lea21st/rmq"
)

func TestTest(t *testing.T) {
	Init()
	ctx := context.TODO()
	for i := 0; i < 10000; i++ {
		if msg, err := rmq.NewMsg().SetCallback(Test, map[string]any{
			"a": 1,
			"b": 2,
		}); err != nil {
			fmt.Printf("消息生成失败:%s\n", err)
			continue
		} else {
			_, err = queue.Push(ctx, msg)
			fmt.Printf("%s\n", err)
		}
	}
}

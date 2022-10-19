package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/lea21st/rmq"
)

func TestTest(t *testing.T) {
	Init()
	data, _ := json.Marshal(queue.Tasks())
	fmt.Println(string(data))
	ctx := context.TODO()
	for i := 0; i < 10000; i++ {
		// if msg, err := rmq.NewMsg().SetCallback(fmt.Sprintf("test%d", i%2+1), map[string]any{
		// 	"a": 1,
		// 	"b": 2,
		// }); err != nil {
		// 	fmt.Printf("消息生成失败:%s\n", err)
		// 	continue
		// } else {
		// 	_, err = queue.Push(ctx, msg)
		// 	fmt.Printf("%s\n", err)
		// }

		msg, _ := rmq.NewMsg().SetTask(&TestTask{
			Name: fmt.Sprintf("testTask-%d", i),
			Val:  i * i,
		})
		_, _ = queue.Push(ctx, msg)
	}
}

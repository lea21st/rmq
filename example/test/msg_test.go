package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/lpong/rmq"
)

func TestTest(t *testing.T) {
	Init()
	data, _ := json.Marshal(queue.Tasks())
	fmt.Println(string(data))
	ctx := context.TODO()
	for i := 1; i < 2; i++ {
		// if msg, err := rmq.NewMsg().SetRawTask(fmt.Sprintf("test%d", i%2+1), map[string]any{
		// 	"a": 1,
		// 	"b": 2,
		// }); err != nil {
		// 	fmt.Printf("消息生成失败:%s\n", err)
		// 	continue
		// } else {
		// 	_, err = queue.Push(ctx, msg)
		// 	fmt.Printf("%s\n", err)
		// }

		// msg, _ := rmq.NewMsg().SetTimeout(3 * time.Second).SetTask(&TestTask{
		// 	Name: fmt.Sprintf("testTask-%d", i),
		// 	Val:  i * i,
		// })
		// _ = queue.Push(ctx, msg)

		// msg, _ = rmq.NewHttpTaskJsonPost("https://www.baidu.com/s?ie=UTF-8&wd=baidu", map[string]any{}).Message()
		// _, _ = queue.Push(ctx, msg)
		// msg, _ := rmq.NewHttpTaskGet("https://www.baidu.com/s?ie=UTF-8&wd=%d", i).Message()
		// _, _ = queue.Push(ctx, msg)

		msg, _ := rmq.NewMsg().SetTimeout(3 * time.Second).SetTask(rmq.NewCommandTask("sh", "cd /tmp", "ls -ash"))
		_ = queue.Push(ctx, msg)
	}
}

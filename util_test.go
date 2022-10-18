package rmq

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestGetTaskInfo(t *testing.T) {
	info := register.GetTaskInfo(&redis.Client{})
	fmt.Println(info)

	info = register.GetTaskInfo(redis.Nil)
	fmt.Println(info)

	info = register.GetTaskInfo(redis.NewClient)
	fmt.Println(info)

	info = register.GetTaskInfo(DaemonStart)
	fmt.Println(info)

	info = register.GetTaskInfo(Meta{})
	fmt.Println(info)

	info = register.GetTaskInfo(&Meta{})
	fmt.Println(info)

}

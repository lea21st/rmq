package rmq

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis/v8"
)

func TestGetTaskInfo(t *testing.T) {
	rType, path := GetTaskInfo(&redis.Client{})
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

	rType, path = GetTaskInfo(redis.Nil)
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

	rType, path = GetTaskInfo(redis.NewClient)
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

	rType, path = GetTaskInfo(DaemonStart)
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

	rType, path = GetTaskInfo(Meta{})
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

	rType, path = GetTaskInfo(&Meta{})
	fmt.Printf("rType=%+v, path=%s\n", rType, path)

}

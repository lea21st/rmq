package rmq

import (
	"fmt"
	"log"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"
)

func DaemonStart(rebootDuration time.Duration, action func()) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			log.Printf("任务异常:%s,将在%s后重启", err, rebootDuration)
			// 等一会重启
			time.AfterFunc(rebootDuration, func() {
				action()
			})
		}
	}()
	action()
}

func GetTaskInfo(v any) (rType reflect.Type, path string) {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Func {
		path = runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name()
	} else {
		path = fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
	}
	rType = t
	return
}

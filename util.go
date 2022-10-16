package rmq

import (
	"log"
	"runtime/debug"
	"time"
)

func DaemonStart(rebootDuration time.Duration, action func()) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			log.Printf("队列异常:%s,将在%s后重启", err, rebootDuration)
			// 等一会重启
			time.AfterFunc(rebootDuration, func() {
				action()
			})
		}
	}()
	action()
}

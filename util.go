package rmq

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
)

func Protect(f func() error) (err error) {
	defer func() {
		errX := recover()
		if errX != nil {
			err = fmt.Errorf("%s\n%v", debug.Stack(), errX)
		}
	}()
	err = f()
	return
}

func Json(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

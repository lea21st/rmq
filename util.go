package rmq

import (
	"encoding/json"
	"fmt"
	"path"
	"runtime"
)

func Protect(f func() error) (err error) {
	defer func() {
		errX := recover()
		if errX != nil {
			var str string
			if pc, file, lineNo, ok := runtime.Caller(2); ok {
				funcName := runtime.FuncForPC(pc).Name()
				fileName := path.Base(file)
				str = fmt.Sprintf("func:%s, file:%s, line:%d ", funcName, fileName, lineNo)
			}
			switch e := errX.(type) {
			case runtime.Error: // 运行时错误
				err = fmt.Errorf("runtime error %s %s", str, e.Error())
			default: // 非运行时错误
				err = fmt.Errorf("panic error %s %v", str, e)
			}
		}
	}()
	err = f()
	return
}

func Json(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}

package rmq

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

type Register struct {
	cache map[any]*TaskInfo
	sync.RWMutex
}

var register = Register{
	cache:   make(map[any]*TaskInfo),
	RWMutex: sync.RWMutex{},
}

func getType(v any) (t reflect.Type) {
	t = reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return
}

func (r *Register) GetTaskInfo(task any) *TaskInfo {
	t := getType(task)
	return r.cache[t]
}

func (r *Register) Register(name string, v any) {
	r.Lock()
	defer r.Unlock()
	t := getType(v)
	var path string
	if t.Kind() == reflect.Func {
		path = runtime.FuncForPC(reflect.ValueOf(v).Pointer()).Name()
	} else {
		path = fmt.Sprintf("%s.%s", t.PkgPath(), t.Name())
	}

	info := &TaskInfo{
		Name:        name,
		Path:        path,
		ReflectType: t,
		IsFunc:      t.Kind() == reflect.Func,
	}
	r.cache[t] = info
	r.cache[name] = info
	r.cache[path] = info
}

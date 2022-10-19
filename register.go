package rmq

import (
	"fmt"
	"reflect"
	"sync"
)

type Register struct {
	cache map[string]*TaskInfo
	sync.RWMutex
}

var register = &Register{
	cache:   make(map[string]*TaskInfo),
	RWMutex: sync.RWMutex{},
}

func (r *Register) CreateTask(name string) (task Task, v *TaskInfo, err error) {
	var ok bool
	if v, ok = r.cache[name]; !ok {
		err = fmt.Errorf("任务%s未注册", name)
		return
	}
	if v.IsCallback {
		task = newFuncTask(v.Name, v.Callback)
		return
	}
	if task, ok = reflect.New(v.ReflectType).Interface().(Task); !ok {
		err = fmt.Errorf("任务%s不是一个有效的Task", name)
		return
	}
	return
}
func (r *Register) Register(v Task) {
	r.Lock()
	defer r.Unlock()
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	r.cache[v.TaskName()] = &TaskInfo{
		Name:        v.TaskName(),
		IsCallback:  false,
		ReflectType: t,
		Callback:    nil,
	}
}

func (r *Register) RegisterFunc(name string, callback Callback) {
	r.Lock()
	defer r.Unlock()
	r.cache[name] = &TaskInfo{
		Name:       name,
		IsCallback: true,
		Callback:   callback,
	}
}

func (r *Register) AllTask() map[string]*TaskInfo {
	return r.cache
}

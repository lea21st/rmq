package rmq

import (
	"log"
	"os"
)

type defaultLog struct {
	entity *log.Logger
}

func newDefaultLog() *defaultLog {
	entity := log.New(os.Stdout, "", log.Lshortfile|log.Ldate|log.Ltime)
	return &defaultLog{entity: entity}
}
func (d *defaultLog) Errorf(format string, arg ...interface{}) {
	d.entity.Printf(format, arg...)
}

func (d *defaultLog) Warningf(format string, arg ...interface{}) {
	d.entity.Printf(format, arg...)
}

func (d *defaultLog) Infof(format string, arg ...interface{}) {
	d.entity.Printf(format, arg...)
}

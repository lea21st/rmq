package rmq

type Logger interface {
	Errorf(format string, arg ...interface{})
	Warningf(format string, args ...interface{})
	Infof(format string, arg ...interface{})
}

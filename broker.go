package rmq

import (
	"context"
	"fmt"
)

type Broker interface {
	Encode(*Message) ([]byte, error)
	Decode(bytes []byte) (msg *Message, err error)

	Push(context.Context, ...*Message) (int64, error)
	Pop(ctx context.Context) (msg *Message, err error)
}

type BrokerBeforeStart interface {
	BeforeStart(ctx context.Context) error
}
type BrokerAfterStart interface {
	AfterStart(ctx context.Context) error
}
type BrokerBeforeExit interface {
	BeforeExit(ctx context.Context) error
}
type BrokerAfterExit interface {
	AfterExit(ctx context.Context) error
}

func BrokerHookProtect(ctx context.Context, f func(ctx context.Context) error) (err error) {
	defer func() {
		if errX := recover(); errX != nil {
			err = fmt.Errorf("%s", errX)
			return
		}
	}()
	return f(ctx)
}

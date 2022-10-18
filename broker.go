package rmq

import (
	"context"
)

type Broker interface {
	Encode(*Message) ([]byte, error)
	Decode(bytes []byte) (msg *Message, err error)

	Push(context.Context, ...*Message) (int64, error)
	Pop(ctx context.Context) (msg *Message, err error)
}

type BrokerHook interface {
	BeforeStart()
	AfterStart()
	BeforeExit()
	AfterExit()
}

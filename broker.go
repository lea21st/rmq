package rmq

import (
	"context"
)

type Broker interface {
	Start(ctx context.Context) chan *Message
	Encode(*Message) ([]byte, error)
	Decode([]byte, *Message) error
	Push(context.Context, ...*Message) (int64, error)
}

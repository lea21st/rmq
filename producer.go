package rmq

import "context"

type Producer interface {
	Push(ctx context.Context)
}

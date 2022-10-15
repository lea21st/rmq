package rmq

import "context"

type HookFunc func(ctx context.Context, msg *Message, resp []byte, err error)

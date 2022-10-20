package rmq

type BatchError struct {
	errs []error
}

func (b *BatchError) HasError() bool {
	return len(b.errs) == 0
}

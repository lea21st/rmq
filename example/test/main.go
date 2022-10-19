package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	ch := make(chan int)

	ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
	defer cancel()

	go Test(ctx)
	ch <- 1
}

func Test(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("ok exit")
			return
		default:
			fmt.Println("ok")

		}
	}
}

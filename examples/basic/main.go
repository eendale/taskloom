package main

import (
	"context"
	"fmt"
	"time"

	"github.com/eendale/taskloom"
)

func main() {
	p, err := taskloom.New(taskloom.Config{
		Workers:  2,
		Capacity: 16,
	})
	if err != nil {
		panic(err)
	}
	if err := p.Start(); err != nil {
		panic(err)
	}

	for i := 1; i <= 5; i++ {
		n := i
		err := p.Submit(taskloom.Func(func(ctx context.Context) error {
			fmt.Println("job", n)
			return nil
		}))
		if err != nil {
			panic(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("stats: %+v\n", p.Stats())
}

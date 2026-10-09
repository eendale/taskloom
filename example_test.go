package taskloom_test

import (
	"context"
	"fmt"

	"github.com/eendale/taskloom"
)

func ExampleProcessor() {
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
	defer p.Close()

	_ = p.Go(func(ctx context.Context) error {
		fmt.Println("done")
		return nil
	})

	_ = p.Wait(context.Background())
	// Output: done
}

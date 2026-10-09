# taskloom

Lightweight in-memory background job processing for Go.

Taskloom runs jobs on a bounded queue with a fixed worker pool. It focuses on a small API, clear lifecycle rules, and predictable shutdown behavior.

## Install

```bash
go get github.com/eendale/taskloom
```

## Quick start

```go
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
		Capacity: 64,
	})
	if err != nil {
		panic(err)
	}
	if err := p.Start(); err != nil {
		panic(err)
	}
	defer p.Shutdown(context.Background())

	_ = p.Submit(taskloom.Func(func(ctx context.Context) error {
		fmt.Println("hello from a job")
		return nil
	}))

	time.Sleep(100 * time.Millisecond)
}
```

## Behavior

- **Submit success** means the job was accepted into memory only. Nothing is written to disk.
- **Queue full** returns `ErrQueueFull` immediately. Submit does not block.
- **Job errors** are counted in stats. They do not stop the processor.
- **Panics** in jobs are recovered so workers keep running.
- **Shutdown** stops new submissions, then waits for queued and in-flight work. If the shutdown context deadline is hit, remaining queued jobs are dropped and `Shutdown` returns the context error. In-flight jobs are cancelled via context; Shutdown still waits for them to return.
- Jobs must observe their context to stop promptly. Taskloom does not claim exactly-once execution.

## Tests

```bash
go test ./...
go test -race ./...
```

## Status

In-memory processor only. Retries, persistence, scheduling, and metrics hooks are out of scope for now.

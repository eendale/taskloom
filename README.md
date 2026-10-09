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

	"github.com/eendale/taskloom"
)

func main() {
	p, err := taskloom.New(taskloom.Config{Workers: 2, Capacity: 64})
	if err != nil {
		panic(err)
	}
	if err := p.Start(); err != nil {
		panic(err)
	}
	defer p.Close()

	_ = p.Go(func(ctx context.Context) error {
		fmt.Println("hello from a job")
		return nil
	})

	_ = p.Wait(context.Background())
}
```

## API highlights

| Method | Behavior |
|--------|----------|
| `Go` / `Submit` | Non-blocking enqueue; `ErrQueueFull` when saturated |
| `GoWait` / `SubmitWait` | Block until queued, context done, or shutdown |
| `Wait` | Wait for accepted jobs without shutting down |
| `StartContext` | Derive job contexts from a parent context |
| `Close` / `Shutdown` | Drain work; `Shutdown` may drop the queue on deadline |

Defaults: `Workers` → `runtime.NumCPU()`, `Capacity` → 64. Optional: `JobTimeout`, `ErrorHandler`.

## Behavior

- **Submit success** means accepted into memory only — not durable.
- **Job errors / panics** are counted, optionally reported via `ErrorHandler`, and never stop the processor.
- **Shutdown** with a deadline drops remaining queued jobs (`Stats.Dropped`) and cancels in-flight work; it still waits for in-flight `Run` calls to return.
- Jobs must observe their context. No exactly-once guarantee.

## Tests

```bash
go test ./...
go test -race ./...
```

## Status

In-memory processor only. Retries, persistence, and scheduling are out of scope for now.

package taskloom

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startProcessor(t *testing.T, cfg Config) *Processor {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return p
}

func TestSubmitAndComplete(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 4})

	done := make(chan struct{})
	err := p.Submit(Func(func(ctx context.Context) error {
		close(done)
		return nil
	}))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not run")
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stats := p.Stats()
	if stats.Submitted != 1 || stats.Completed != 1 || stats.Failed != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestJobFailure(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 4})

	done := make(chan struct{})
	_ = p.Submit(Func(func(ctx context.Context) error {
		defer close(done)
		return errors.New("boom")
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not run")
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stats := p.Stats()
	if stats.Failed != 1 || stats.Completed != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestMultipleWorkers(t *testing.T) {
	p := startProcessor(t, Config{Workers: 4, Capacity: 32})

	const n = 20
	var ran atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		err := p.Submit(Func(func(ctx context.Context) error {
			defer wg.Done()
			ran.Add(1)
			return nil
		}))
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("jobs did not finish")
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if ran.Load() != n {
		t.Fatalf("ran = %d, want %d", ran.Load(), n)
	}
}

func TestQueueFull(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 1})

	block := make(chan struct{})
	release := make(chan struct{})

	// Occupy the worker.
	if err := p.Submit(Func(func(ctx context.Context) error {
		close(block)
		<-release
		return nil
	})); err != nil {
		t.Fatalf("Submit blocking job: %v", err)
	}

	select {
	case <-block:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking job did not start")
	}

	// Fill the queue.
	if err := p.Submit(Func(func(ctx context.Context) error { return nil })); err != nil {
		t.Fatalf("Submit queued job: %v", err)
	}

	if err := p.Submit(Func(func(ctx context.Context) error { return nil })); err != ErrQueueFull {
		t.Fatalf("Submit = %v, want ErrQueueFull", err)
	}

	close(release)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestSubmitNotStarted(t *testing.T) {
	p, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Submit(Func(func(ctx context.Context) error { return nil })); err != ErrNotStarted {
		t.Fatalf("Submit = %v, want ErrNotStarted", err)
	}
}

func TestContextCancellation(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 4})

	started := make(chan struct{})
	cancelled := make(chan struct{})

	_ = p.Submit(Func(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := p.Shutdown(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("Shutdown = %v, want deadline exceeded", err)
	}

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("job was not cancelled")
	}
}

func TestGracefulShutdown(t *testing.T) {
	p := startProcessor(t, Config{Workers: 2, Capacity: 8})

	var ran atomic.Int64
	for i := 0; i < 5; i++ {
		_ = p.Submit(Func(func(ctx context.Context) error {
			ran.Add(1)
			return nil
		}))
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if ran.Load() != 5 {
		t.Fatalf("ran = %d, want 5", ran.Load())
	}
	if err := p.Submit(Func(func(ctx context.Context) error { return nil })); err != ErrClosed {
		t.Fatalf("Submit after shutdown = %v, want ErrClosed", err)
	}
}

func TestShutdownDeadlineDropsQueued(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 8})

	started := make(chan struct{})
	release := make(chan struct{})

	_ = p.Submit(Func(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first job did not start")
	}

	var queuedRan atomic.Int64
	for i := 0; i < 3; i++ {
		_ = p.Submit(Func(func(ctx context.Context) error {
			queuedRan.Add(1)
			return nil
		}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := p.Shutdown(ctx)
	close(release)

	if err != context.DeadlineExceeded {
		t.Fatalf("Shutdown = %v, want deadline exceeded", err)
	}
	if queuedRan.Load() != 0 {
		t.Fatalf("queued jobs ran = %d, want 0", queuedRan.Load())
	}
}

func TestConcurrentSubmitAndShutdown(t *testing.T) {
	p := startProcessor(t, Config{Workers: 4, Capacity: 64})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = p.Submit(Func(func(ctx context.Context) error {
					return nil
				}))
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = p.Shutdown(context.Background())
	}()

	wg.Wait()

	if err := p.Submit(Func(func(ctx context.Context) error { return nil })); err != ErrClosed {
		t.Fatalf("Submit after shutdown = %v, want ErrClosed", err)
	}
}

func TestPanicIsolated(t *testing.T) {
	p := startProcessor(t, Config{Workers: 1, Capacity: 4})

	_ = p.Submit(Func(func(ctx context.Context) error {
		panic("nope")
	}))

	ran := make(chan struct{})
	_ = p.Submit(Func(func(ctx context.Context) error {
		close(ran)
		return nil
	}))

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("processor did not recover from panic")
	}

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	stats := p.Stats()
	if stats.Failed < 1 || stats.Completed < 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

package taskloom

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrQueueFull is returned by Submit when the queue is at capacity.
	ErrQueueFull = errors.New("taskloom: queue full")

	// ErrClosed is returned by Submit after Shutdown has started.
	ErrClosed = errors.New("taskloom: processor closed")

	// ErrNotStarted is returned by Submit or Shutdown before Start.
	ErrNotStarted = errors.New("taskloom: processor not started")

	// ErrAlreadyStarted is returned by Start if called more than once.
	ErrAlreadyStarted = errors.New("taskloom: already started")
)

const (
	defaultWorkers  = 1
	defaultCapacity = 64
)

// Job is work executed by a worker. Cancellation is cooperative:
// Run must observe ctx to stop promptly.
type Job interface {
	Run(ctx context.Context) error
}

// Func adapts a function to the Job interface.
type Func func(ctx context.Context) error

// Run calls f(ctx).
func (f Func) Run(ctx context.Context) error {
	return f(ctx)
}

// Config controls processor capacity and concurrency.
// Zero values are replaced by defaults.
type Config struct {
	Workers  int // concurrent workers; default 1
	Capacity int // max queued jobs (not counting in-flight); default 64
}

// Stats is a point-in-time snapshot.
type Stats struct {
	Submitted uint64
	Completed uint64
	Failed    uint64
	InFlight  int
	Queued    int
}

// Processor accepts jobs and runs them in the background.
// It is in-memory only: a successful Submit means the job was
// accepted into memory, not durably stored. Taskloom does not
// guarantee exactly-once execution.
type Processor struct {
	workers  int
	capacity int

	queue chan Job

	mu      sync.Mutex
	started bool
	closed  bool

	runCtx    context.Context
	runCancel context.CancelFunc

	workerWG sync.WaitGroup
	jobWG    sync.WaitGroup

	submitted atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64
	inFlight  atomic.Int64
}

// New creates a stopped processor. Call Start before Submit.
func New(cfg Config) (*Processor, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = defaultCapacity
	}

	return &Processor{
		workers:  workers,
		capacity: capacity,
		queue:    make(chan Job, capacity),
	}, nil
}

// Start launches worker goroutines. Safe to call once.
func (p *Processor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return ErrAlreadyStarted
	}

	p.runCtx, p.runCancel = context.WithCancel(context.Background())
	p.started = true

	for i := 0; i < p.workers; i++ {
		p.workerWG.Add(1)
		go p.worker()
	}
	return nil
}

// Submit enqueues job for later execution.
// A nil error means the job was accepted into memory only.
func (p *Processor) Submit(job Job) error {
	if job == nil {
		return errors.New("taskloom: nil job")
	}

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return ErrNotStarted
	}
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}

	p.jobWG.Add(1)
	select {
	case p.queue <- job:
		p.submitted.Add(1)
		p.mu.Unlock()
		return nil
	default:
		p.jobWG.Done()
		p.mu.Unlock()
		return ErrQueueFull
	}
}

// Stats returns a snapshot of counters and queue depth.
func (p *Processor) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Completed: p.completed.Load(),
		Failed:    p.failed.Load(),
		InFlight:  int(p.inFlight.Load()),
		Queued:    len(p.queue),
	}
}

// Shutdown stops accepting new jobs, then waits for queued and
// in-flight jobs to finish, or until ctx is done.
//
// If ctx expires first, remaining queued jobs are not run and
// Shutdown returns ctx.Err(). In-flight jobs receive cancellation
// via their context; Shutdown still waits for those Run calls to
// return so worker goroutines exit.
func (p *Processor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return ErrNotStarted
	}
	if p.closed {
		p.mu.Unlock()
		p.workerWG.Wait()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.jobWG.Wait()
		close(done)
	}()

	var shutdownErr error
	select {
	case <-done:
	case <-ctx.Done():
		shutdownErr = ctx.Err()
		p.runCancel()
		p.discardQueued()
		<-done
	}

	p.mu.Lock()
	close(p.queue)
	p.mu.Unlock()

	p.workerWG.Wait()
	p.runCancel()
	return shutdownErr
}

func (p *Processor) discardQueued() {
	for {
		select {
		case <-p.queue:
			p.jobWG.Done()
		default:
			return
		}
	}
}

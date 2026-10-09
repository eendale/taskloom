// Package taskloom provides an in-memory background job processor.
//
// Jobs are accepted into a bounded queue and executed by a fixed worker pool.
// A successful Submit means the job is held in memory only — not durable and
// not exactly-once. Jobs must observe context cancellation to stop promptly.
package taskloom

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueFull      = errors.New("taskloom: queue full")
	ErrClosed         = errors.New("taskloom: processor closed")
	ErrNotStarted     = errors.New("taskloom: processor not started")
	ErrAlreadyStarted = errors.New("taskloom: already started")
	ErrNilJob         = errors.New("taskloom: nil job")
)

const defaultCapacity = 64

type Job interface {
	Run(ctx context.Context) error
}

type Func func(ctx context.Context) error

func (f Func) Run(ctx context.Context) error { return f(ctx) }

// ErrorHandler is called after a job returns an error or panics.
// It must not block; it runs on a worker goroutine.
type ErrorHandler func(err error)

type Config struct {
	Workers      int           // <=0 means runtime.NumCPU()
	Capacity     int           // <=0 means 64
	JobTimeout   time.Duration // 0 means no per-job timeout
	ErrorHandler ErrorHandler
}

type Stats struct {
	Submitted uint64
	Completed uint64
	Failed    uint64
	Dropped   uint64
	InFlight  int
	Queued    int
}

type Processor struct {
	workers    int
	capacity   int
	jobTimeout time.Duration
	onError    ErrorHandler

	queue   chan Job
	closing chan struct{}

	mu      sync.Mutex
	started bool
	closed  bool

	runCtx    context.Context
	runCancel context.CancelFunc

	workerWG sync.WaitGroup
	jobWG    sync.WaitGroup
	submitWG sync.WaitGroup

	submitted atomic.Uint64
	completed atomic.Uint64
	failed    atomic.Uint64
	dropped   atomic.Uint64
	inFlight  atomic.Int64
}

func New(cfg Config) (*Processor, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}
	}
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = defaultCapacity
	}

	return &Processor{
		workers:    workers,
		capacity:   capacity,
		jobTimeout: cfg.JobTimeout,
		onError:    cfg.ErrorHandler,
		queue:      make(chan Job, capacity),
		closing:    make(chan struct{}),
	}, nil
}

// Start launches workers with a background context.
func (p *Processor) Start() error {
	return p.StartContext(context.Background())
}

// StartContext launches workers. Job contexts are derived from ctx;
// cancelling ctx cancels in-flight jobs but does not shut down the processor.
func (p *Processor) StartContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return ErrAlreadyStarted
	}

	p.runCtx, p.runCancel = context.WithCancel(ctx)
	p.started = true

	for i := 0; i < p.workers; i++ {
		p.workerWG.Add(1)
		go p.worker()
	}
	return nil
}

func (p *Processor) Submit(job Job) error {
	if job == nil {
		return ErrNilJob
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

// SubmitWait blocks until the job is queued, ctx is done, or the processor closes.
func (p *Processor) SubmitWait(ctx context.Context, job Job) error {
	if job == nil {
		return ErrNilJob
	}
	if ctx == nil {
		ctx = context.Background()
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
	p.submitWG.Add(1)
	p.jobWG.Add(1)
	p.mu.Unlock()
	defer p.submitWG.Done()

	select {
	case p.queue <- job:
		p.submitted.Add(1)
		return nil
	case <-ctx.Done():
		p.jobWG.Done()
		return ctx.Err()
	case <-p.closing:
		p.jobWG.Done()
		return ErrClosed
	}
}

// Go submits fn as a job. It matches the ergonomics of errgroup.Group.Go.
func (p *Processor) Go(fn func(ctx context.Context) error) error {
	if fn == nil {
		return ErrNilJob
	}
	return p.Submit(Func(fn))
}

// GoWait is SubmitWait for a function job.
func (p *Processor) GoWait(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return ErrNilJob
	}
	return p.SubmitWait(ctx, Func(fn))
}

// Wait blocks until all accepted jobs finish or ctx is done.
func (p *Processor) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return ErrNotStarted
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.jobWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Processor) Stats() Stats {
	return Stats{
		Submitted: p.submitted.Load(),
		Completed: p.completed.Load(),
		Failed:    p.failed.Load(),
		Dropped:   p.dropped.Load(),
		InFlight:  int(p.inFlight.Load()),
		Queued:    len(p.queue),
	}
}

// Shutdown stops submissions and drains work, or drops the queue if ctx is done.
func (p *Processor) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

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
	close(p.closing)
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

	p.submitWG.Wait()

	p.mu.Lock()
	close(p.queue)
	p.mu.Unlock()

	p.workerWG.Wait()
	p.runCancel()
	return shutdownErr
}

// Close shuts down with a background context. Safe to defer before Start.
func (p *Processor) Close() error {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()
	if !started {
		return nil
	}
	return p.Shutdown(context.Background())
}

func (p *Processor) discardQueued() {
	for {
		select {
		case <-p.queue:
			p.dropped.Add(1)
			p.jobWG.Done()
		default:
			return
		}
	}
}

func (p *Processor) reportError(err error) {
	if p.onError == nil || err == nil {
		return
	}
	defer func() { _ = recover() }()
	p.onError(err)
}

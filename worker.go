package taskloom

import (
	"context"
	"fmt"
)

func (p *Processor) worker() {
	defer p.workerWG.Done()

	for job := range p.queue {
		p.inFlight.Add(1)
		p.runJob(job)
		p.inFlight.Add(-1)
		p.jobWG.Done()
	}
}

func (p *Processor) runJob(job Job) {
	defer func() {
		if r := recover(); r != nil {
			p.failed.Add(1)
			p.reportError(fmt.Errorf("taskloom: panic: %v", r))
		}
	}()

	ctx := p.runCtx
	if p.jobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(p.runCtx, p.jobTimeout)
		defer cancel()
	}

	if err := job.Run(ctx); err != nil {
		p.failed.Add(1)
		p.reportError(err)
		return
	}
	p.completed.Add(1)
}

package taskloom

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
		if recover() != nil {
			p.failed.Add(1)
		}
	}()

	err := job.Run(p.runCtx)
	if err != nil {
		p.failed.Add(1)
		return
	}
	p.completed.Add(1)
}

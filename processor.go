package qless

type Processor struct {
	cfg     normalizedConfig
	handler Handler
	obs     *observability

	queue         chan *job

	// ... and other state that needs to be known to the qless processor	 like...
	// 1. how many payloads are currently in use (waiting in the queue or being executed?)
	// 2. how many jobs are currently active?
	// 3. what is the state of the processor (starting up, accepting new jobs, shutting down, stopped, etc?)
	// 4. how many jobs are currently pending enqueue requests? (during graceful shutdown we wait for all of these to complete)
	// ... and so on idk
}

// Handler is the user's function passed in to the processor that will be executed for each request.
type Handler func(context.Context, []byte) error

// NewProcessor creates a new processor with the given config and handler.
func NewProcessor(cfg Config, handler Handler) *Processor {
	// 1. validate & normalize config
	// 2. create new observability instance
	// 3. return new Processor instance with state "starting"
}

// Start starts the worker pool which will begin executing jobs as they are received.
func (p *Processor) Start() error {
	// 1. start the worker pool (application handles spinning up the HTTP server and calling this function)
	// 2. Processor state "running"
}

// HTTPHandler returns the handler that is used at the application level
func (p *Processor) HTTPHandler() http.Handler {
	return http.HandlerFunc(p.serveHTTP)
}

// Shutdown stops accepting new work and waits for accepted work to finish.
// If ctx expires, active work is cancelled and ShutdownError is returned.
func (p *Processor) Shutdown(ctx context.Context) error {
	// shutdown the processor
	// 1. stop accepting new jobs
	// 2. abandon jobs in the queue (ensure emit metrics and logs)
	// 3. wait for jobs currently working to complete
	// 4. return any errors that occurred during the shutdown process
	return nil
}

func (p *Processor) newJob(payload []byte) *job {
	// simply return a new job struct with the payload
}

// execute runs the job in a free worker slot and handles retries
func (p *Processor) execute(j *job) {
}

// and other functions that are used internally to the processor like
// enqueue, abandon, start workers, reserve worker, release worker, etc.
 
package executor

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

// FirstResponseTimeoutError is a local latency-policy decision, not evidence
// that the selected credential is invalid or that its upstream quota is spent.
// It deliberately does not unwrap context cancellation or implement
// RequestScopedError: an attempt timeout can safely rotate before commitment.
// Final marks a request-wide budget/attempt limit rather than one attempt.
type FirstResponseTimeoutError struct{ Final bool }

func (e *FirstResponseTimeoutError) Error() string {
	if e.Final {
		return `{"error":{"code":"first_response_timeout","type":"timeout_error","message":"CPA first response waiting budget exhausted"}}`
	}
	return `{"error":{"code":"first_response_attempt_timeout","type":"timeout_error","message":"CPA first response attempt timed out before output"}}`
}
func (*FirstResponseTimeoutError) StatusCode() int { return http.StatusGatewayTimeout }
func IsFirstResponseTimeout(err error) bool {
	var timeout *FirstResponseTimeoutError
	return errors.As(err, &timeout)
}

// FirstResponseBufferLimitError ends an uncommitted response whose preamble
// cannot be retained safely. It is terminal rather than replaying a request
// after an unknown or excessively large upstream preamble.
type FirstResponseBufferLimitError struct{}

func (*FirstResponseBufferLimitError) Error() string {
	return `{"error":{"code":"first_response_buffer_limit","type":"proxy_error","message":"CPA first response preamble exceeded its buffer limit"}}`
}
func (*FirstResponseBufferLimitError) StatusCode() int       { return http.StatusBadGateway }
func (*FirstResponseBufferLimitError) IsRequestScoped() bool { return true }
func IsFirstResponsePolicyError(err error) bool {
	var bufferLimit *FirstResponseBufferLimitError
	var committed *FirstResponseCommittedError
	return IsFirstResponseTimeout(err) || errors.As(err, &bufferLimit) || errors.As(err, &committed)
}

// FirstResponseCommittedError rejects a new execution after upstream progress
// made the request unsafe to replay. It is not a latency timeout.
type FirstResponseCommittedError struct{}

func (*FirstResponseCommittedError) Error() string {
	return `{"error":{"code":"first_response_already_committed","type":"proxy_error","message":"CPA cannot replay a request after upstream output or tool execution started"}}`
}
func (*FirstResponseCommittedError) StatusCode() int       { return http.StatusConflict }
func (*FirstResponseCommittedError) IsRequestScoped() bool { return true }

// FirstResponseBudget shares an absolute pre-response deadline and at most two
// HTTP attempts across credential selection and retry rounds. Merely attaching
// it does not activate the policy: the HTTP executor must first inspect its
// finalized payload and either StartAttempt or Disable it.
type FirstResponseBudget struct {
	mu             sync.Mutex
	deadline       time.Time
	attemptTimeout time.Duration
	disabled       bool
	committed      bool
	active         bool
	attempts       int
	current        *FirstResponseAttempt
}

type firstResponseBudgetContextKey struct{}

func NewFirstResponseBudget(total, attempt time.Duration) *FirstResponseBudget {
	if attempt <= 0 || attempt > total {
		attempt = total
	}
	return &FirstResponseBudget{deadline: time.Now().Add(total), attemptTimeout: attempt, disabled: total <= 0}
}
func WithFirstResponseBudget(ctx context.Context, b *FirstResponseBudget) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, firstResponseBudgetContextKey{}, b)
}
func FirstResponseBudgetFromContext(ctx context.Context) *FirstResponseBudget {
	if ctx == nil {
		return nil
	}
	b, _ := ctx.Value(firstResponseBudgetContextKey{}).(*FirstResponseBudget)
	return b
}
func (b *FirstResponseBudget) Active() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.active && !b.disabled && !b.committed
}

// Committed reports irreversible upstream progress, independently of whether
// timers were disabled. It fences every retry layer, including translation
// and interceptor paths that produced no downstream bytes.
func (b *FirstResponseBudget) Committed() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committed
}
func (b *FirstResponseBudget) Check() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active || b.disabled || b.committed {
		return nil
	}
	return b.checkLocked()
}
func (b *FirstResponseBudget) checkLocked() error {
	if !time.Now().Before(b.deadline) || b.attempts >= 2 {
		return &FirstResponseTimeoutError{Final: true}
	}
	return nil
}
func (b *FirstResponseBudget) Disable() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.disabled = true
	if b.current != nil && b.current.timer != nil {
		b.current.timer.Stop()
	}
}

// ControlContext bounds credential acquisition and cooldown waits after the
// final payload has activated the policy. It must never own a response stream.
func (b *FirstResponseBudget) ControlContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if b == nil {
		return parent, func() {}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active || b.disabled || b.committed {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	timer := time.AfterFunc(time.Until(b.deadline), func() { cancel(&FirstResponseTimeoutError{Final: true}) })
	return ctx, func() { timer.Stop(); cancel(context.Canceled) }
}

// StartAttempt creates a cancellable HTTP context, never a permanent context
// deadline: Commit can disarm the timer while a successful stream continues.
func (b *FirstResponseBudget) StartAttempt(parent context.Context) (context.Context, *FirstResponseAttempt, error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return parent, nil, err
	}
	if b == nil {
		return parent, nil, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.committed {
		return parent, nil, &FirstResponseCommittedError{}
	}
	if b.disabled {
		return parent, nil, nil
	}
	b.active = true
	if err := b.checkLocked(); err != nil {
		return parent, nil, err
	}
	if b.current != nil && b.current.state == attemptWaiting {
		return parent, nil, errors.New("first response attempt still active")
	}
	b.attempts++
	ctx, cancel := context.WithCancelCause(parent)
	a := &FirstResponseAttempt{budget: b, parent: parent, ctx: ctx, cancel: cancel}
	b.current = a
	remaining := time.Until(b.deadline)
	limit := min(b.attemptTimeout, remaining)
	a.deadline = time.Now().Add(limit)
	a.timer = time.AfterFunc(limit, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if a.state != attemptWaiting || b.disabled || b.committed || parent.Err() != nil {
			return
		}
		a.state = attemptTimedOut
		a.failure = &FirstResponseTimeoutError{Final: !time.Now().Before(b.deadline)}
		cancel(a.failure)
	})
	return ctx, a, nil
}

const (
	attemptWaiting uint8 = iota
	attemptCommitted
	attemptTimedOut
	attemptClosed
)

type FirstResponseAttempt struct {
	budget   *FirstResponseBudget
	parent   context.Context
	ctx      context.Context
	cancel   context.CancelCauseFunc
	timer    *time.Timer
	state    uint8
	failure  error
	deadline time.Time
}

// Commit atomically chooses progress over timeout and disarms the entire
// request budget. A false result requires discarding the late upstream frame.
func (a *FirstResponseAttempt) Commit() bool {
	if a == nil {
		return true
	}
	b := a.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if a.state != attemptWaiting || a.parent.Err() != nil || a.ctx.Err() != nil {
		return a.state == attemptCommitted
	}
	if !time.Now().Before(a.deadline) {
		a.state = attemptTimedOut
		a.failure = &FirstResponseTimeoutError{Final: !time.Now().Before(b.deadline)}
		a.timer.Stop()
		a.cancel(a.failure)
		return false
	}
	a.state = attemptCommitted
	b.committed = true
	a.timer.Stop()
	return true
}
func (a *FirstResponseAttempt) Pending() bool {
	if a == nil {
		return false
	}
	b := a.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	return a.state == attemptWaiting && !b.disabled && !b.committed
}

// Err normalizes only this attempt's locally caused cancellation. Caller and
// Home lifecycle cancellation retain their original identity.
func (a *FirstResponseAttempt) Err(err error) error {
	if a == nil {
		return err
	}
	b := a.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if parentErr := a.parent.Err(); parentErr != nil {
		return parentErr
	}
	if a.failure != nil {
		return a.failure
	}
	return err
}

// Close releases the attempt context after transport/body ownership has ended.
// Calling it on function return after handing off a live stream would cancel
// that stream; successful executors must defer Close in their stream goroutine.
func (a *FirstResponseAttempt) Close() {
	if a == nil {
		return
	}
	b := a.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	a.timer.Stop()
	if a.state != attemptTimedOut {
		a.state = attemptClosed
	}
	a.cancel(context.Canceled)
}

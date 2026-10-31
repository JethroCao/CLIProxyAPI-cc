package auth

import (
	"context"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// PrepareFirstResponseBudget attaches one pending policy to an entire request,
// including API-handler bootstrap retries. Only the Codex HTTP executor can
// activate it after checking its finalized payload; other paths ignore it.
func (m *Manager) PrepareFirstResponseBudget(ctx context.Context) context.Context {
	if cliproxyexecutor.FirstResponseBudgetFromContext(ctx) != nil || cliproxyexecutor.DownstreamWebsocket(ctx) {
		return ctx
	}
	cfg := m.runtimeConfigSnapshot()
	if cfg == nil || cfg.Codex.FirstResponseTimeoutDuration() <= 0 {
		return ctx
	}
	return cliproxyexecutor.WithFirstResponseBudget(ctx, cliproxyexecutor.NewFirstResponseBudget(cfg.Codex.FirstResponseTimeoutDuration(), cfg.Codex.FirstResponseAttemptTimeoutDuration()))
}

func firstResponseControlContext(ctx context.Context) (context.Context, func() error) {
	controlCtx, cancel := cliproxyexecutor.FirstResponseBudgetFromContext(ctx).ControlContext(ctx)
	return controlCtx, func() error {
		err := context.Cause(controlCtx)
		cancel()
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if cliproxyexecutor.IsFirstResponseTimeout(err) {
			return err
		}
		return nil
	}
}

func waitForCooldownWithFirstResponseBudget(ctx context.Context, wait, maxWait time.Duration) error {
	controlCtx, release := firstResponseControlContext(ctx)
	err := waitForCooldown(controlCtx, wait, maxWait)
	if errBudget := release(); errBudget != nil {
		return errBudget
	}
	return err
}

// checkFirstResponseBudget never replaces a real upstream refusal just because
// two attempts have already been used. Local timer expiry retains its explicit
// local timeout error, while an upstream overload remains an overload.
func checkFirstResponseBudget(ctx context.Context, lastErr error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if cliproxyexecutor.FirstResponseBudgetFromContext(ctx).Committed() {
		if lastErr != nil {
			return lastErr
		}
		return &cliproxyexecutor.FirstResponseCommittedError{}
	}
	if err := cliproxyexecutor.FirstResponseBudgetFromContext(ctx).Check(); err != nil {
		if lastErr != nil && !cliproxyexecutor.IsFirstResponseTimeout(lastErr) {
			return lastErr
		}
		return err
	}
	return nil
}

// committedFirstResponseFailure preserves normal upstream-result accounting
// while stopping every replay path after irreversible progress. Local policy
// failures are handled separately and must never penalize a credential.
func (m *Manager) committedFirstResponseFailure(ctx context.Context, auth *Auth, provider, model, routeModel string, opts cliproxyexecutor.Options, err error, ephemeral bool, executionSeq uint64) (error, bool) {
	if err == nil || !cliproxyexecutor.FirstResponseBudgetFromContext(ctx).Committed() {
		return nil, false
	}
	if ctx != nil && ctx.Err() != nil {
		m.recordCanceledUpstreamCooldown(ctx, auth, provider, model, routeModel, opts, err, ephemeral, executionSeq)
		return ctx.Err(), true
	}
	action, okAction := matchRequestScopedErrorAction(auth, err, m.runtimeConfigSnapshot())
	result := Result{AuthID: auth.ID, Provider: provider, Model: model, RouteModel: routeModel, Success: false, Error: resultErrorFromError(err), Options: opts, CredentialVersion: auth.CredentialVersion, RegistrationEpoch: auth.RegistrationEpoch, ExecutionSeq: executionSeq}
	result.RetryAfter = retryAfterFromError(err)
	result.CredentialScope = isCredentialScopedError(err)
	applyRequestScopedActionToResult(action, okAction, &result)
	m.recordExecutionResult(ctx, result, auth, ephemeral)
	return err, true
}

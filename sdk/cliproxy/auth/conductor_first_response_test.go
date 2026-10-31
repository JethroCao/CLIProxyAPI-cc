package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executionregistry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type firstResponseExecutor struct {
	credentialRetryLimitExecutor
	attempts          []string
	contexts          []context.Context
	alwaysTimeout     bool
	commitError       error
	commitStreamError bool
	commitEmpty       bool
}

func (e *firstResponseExecutor) ExecuteStream(ctx context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.attempts = append(e.attempts, auth.ID)
	e.contexts = append(e.contexts, ctx)
	b := cliproxyexecutor.FirstResponseBudgetFromContext(ctx)
	if b == nil {
		return nil, errors.New("missing first response budget")
	}
	attemptCtx, a, err := b.StartAttempt(ctx)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	if e.commitError != nil || e.commitEmpty {
		if !a.Commit() {
			return nil, a.Err(attemptCtx.Err())
		}
		if e.commitEmpty || e.commitStreamError {
			ch := make(chan cliproxyexecutor.StreamChunk, 1)
			if e.commitError != nil {
				ch <- cliproxyexecutor.StreamChunk{Err: e.commitError}
			}
			close(ch)
			return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
		}
		return nil, e.commitError
	}
	if e.alwaysTimeout || len(e.attempts) == 1 {
		<-attemptCtx.Done()
		return nil, a.Err(attemptCtx.Err())
	}
	if !a.Commit() {
		return nil, a.Err(attemptCtx.Err())
	}
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}
func firstResponseConfig() *internalconfig.Config {
	return &internalconfig.Config{Codex: internalconfig.CodexConfig{FirstResponseTimeout: "30s", FirstResponseAttemptTimeout: "15s"}}
}
func newFirstResponseManager(t *testing.T, alwaysTimeout bool) (*Manager, *firstResponseExecutor) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	m.SetConfig(firstResponseConfig())
	m.SetRetryConfig(10, 0, 10)
	e := &firstResponseExecutor{credentialRetryLimitExecutor: credentialRetryLimitExecutor{id: "codex"}, alwaysTimeout: alwaysTimeout}
	m.RegisterExecutor(e)
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "budget-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "codex", Status: StatusActive}); err != nil {
			t.Fatal(err)
		}
	}
	return m, e
}
func TestManagerFirstResponseTimeoutRotatesWithoutCooldown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, e := newFirstResponseManager(t, false)
		start := time.Now()
		result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if result == nil || result.Chunks == nil {
			t.Fatal("no stream")
		}
		for c := range result.Chunks {
			if c.Err != nil {
				t.Fatal(c.Err)
			}
		}
		if len(e.attempts) != 2 || e.attempts[0] == e.attempts[1] {
			t.Fatalf("attempts=%v", e.attempts)
		}
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Fatalf("elapsed=%v", elapsed)
		}
		auth := m.auths[e.attempts[0]]
		if auth.Unavailable || auth.Status == StatusError || !auth.NextRetryAfter.IsZero() || len(auth.ModelStates) != 0 {
			t.Fatalf("local policy poisoned credential: %#v", auth)
		}
	})
}
func TestManagerFirstResponseTotalBudgetStopsRetryRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, e := newFirstResponseManager(t, true)
		start := time.Now()
		result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if result != nil {
			t.Fatal("timeout became committed stream")
		}
		if !cliproxyexecutor.IsFirstResponseTimeout(err) || statusCodeFromError(err) != 504 {
			t.Fatalf("err=%v", err)
		}
		if elapsed := time.Since(start); elapsed != 30*time.Second {
			t.Fatalf("elapsed=%v", elapsed)
		}
		if len(e.attempts) != 2 {
			t.Fatalf("attempts=%v", e.attempts)
		}
	})
}

type firstResponseHomeDispatcher struct {
	e                  *firstResponseExecutor
	calls              int
	releasedBeforeNext bool
	blockNext          bool
}

func (*firstResponseHomeDispatcher) HeartbeatOK() bool       { return true }
func (*firstResponseHomeDispatcher) AbortAmbiguousDispatch() {}
func (d *firstResponseHomeDispatcher) RPopAuth(ctx context.Context, _ string, _ string, _ http.Header, _ int) ([]byte, error) {
	if d.calls > 0 {
		d.releasedBeforeNext = d.e.contexts[d.calls-1].Err() != nil
		if d.blockNext {
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	d.calls++
	return json.Marshal(homeAuthDispatchResponse{Auth: Auth{ID: []string{"budget-home-a", "budget-home-b", "budget-home-c"}[d.calls-1], Provider: "codex", Status: StatusActive}})
}
func TestManagerFirstResponseHomeReleasesBeforeFailover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := firstResponseConfig()
		cfg.Home.Enabled = true
		m := NewManager(nil, nil, nil)
		m.SetConfig(cfg)
		m.SetRetryConfig(0, 0, 2)
		e := &firstResponseExecutor{credentialRetryLimitExecutor: credentialRetryLimitExecutor{id: "codex"}}
		d := &firstResponseHomeDispatcher{e: e}
		m.RegisterExecutor(e)
		m.PublishHomeDispatch(d, executionregistry.New(), 1)
		result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if err != nil {
			t.Fatal(err)
		}
		for c := range result.Chunks {
			if c.Err != nil {
				t.Fatal(c.Err)
			}
		}
		if len(e.attempts) != 2 || e.attempts[0] == e.attempts[1] || !d.releasedBeforeNext {
			t.Fatalf("attempts=%v release=%v", e.attempts, d.releasedBeforeNext)
		}
	})
}

func TestManagerFirstResponseBudgetBoundsHomeRetryAcquisition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := firstResponseConfig()
		cfg.Home.Enabled = true
		m := NewManager(nil, nil, nil)
		m.SetConfig(cfg)
		m.SetRetryConfig(0, 0, 2)
		e := &firstResponseExecutor{credentialRetryLimitExecutor: credentialRetryLimitExecutor{id: "codex"}}
		d := &firstResponseHomeDispatcher{e: e, blockNext: true}
		m.RegisterExecutor(e)
		m.PublishHomeDispatch(d, executionregistry.New(), 1)
		start := time.Now()
		result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if result != nil || !cliproxyexecutor.IsFirstResponseTimeout(err) {
			t.Fatalf("result=%v err=%v", result, err)
		}
		if elapsed := time.Since(start); elapsed != 30*time.Second {
			t.Fatalf("retry acquisition exceeded budget: %v", elapsed)
		}
		if len(e.attempts) != 1 || !d.releasedBeforeNext {
			t.Fatalf("attempts=%v release=%v", e.attempts, d.releasedBeforeNext)
		}
	})
}

func TestManagerFirstResponseBudgetSurvivesHandlerRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, e := newFirstResponseManager(t, true)
		ctx := m.PrepareFirstResponseBudget(context.Background())
		_, err := m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if !cliproxyexecutor.IsFirstResponseTimeout(err) {
			t.Fatal(err)
		}
		start := time.Now()
		_, err = m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
		if !cliproxyexecutor.IsFirstResponseTimeout(err) || time.Since(start) != 0 || len(e.attempts) != 2 {
			t.Fatalf("outer retry reset budget: %v attempts=%v", err, e.attempts)
		}
	})
}

func TestManagerFirstResponseCommittedOutputNeverReplays(t *testing.T) {
	for _, status := range []int{401, 503} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("status%d/stream%v", status, stream), func(t *testing.T) {
				m, e := newFirstResponseManager(t, false)
				hook := &resultCaptureHook{}
				m.hook = hook
				upstream := &Error{HTTPStatus: status, Message: "upstream failed after server tool"}
				e.commitError = upstream
				e.commitStreamError = stream
				ctx := m.PrepareFirstResponseBudget(context.Background())
				result, err := m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
				if result != nil {
					for c := range result.Chunks {
						if c.Err != nil {
							err = c.Err
						}
					}
				}
				if len(e.attempts) != 1 || !errors.Is(err, upstream) {
					t.Fatalf("committed work replayed or error replaced: calls=%v err=%v", e.attempts, err)
				}
				if results := hook.Results(); len(results) != 1 || results[0].Success || results[0].Error == nil || results[0].Error.HTTPStatus != status {
					t.Fatalf("committed upstream failure was not recorded once: %+v", results)
				}
				_, err = m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
				if len(e.attempts) != 1 || err == nil || cliproxyexecutor.IsFirstResponseTimeout(err) {
					t.Fatalf("new manager entry replayed committed work: calls=%v err=%v", e.attempts, err)
				}
			})
		}
	}
}
func TestManagerFirstResponseCommittedEmptyTranslationNeverReplays(t *testing.T) {
	m, e := newFirstResponseManager(t, false)
	e.commitEmpty = true
	result, err := m.ExecuteStream(context.Background(), []string{"codex"}, cliproxyexecutor.Request{Model: "budget-model"}, cliproxyexecutor.Options{})
	if result != nil {
		for c := range result.Chunks {
			if c.Err != nil {
				err = c.Err
			}
		}
	}
	if len(e.attempts) != 1 || err == nil {
		t.Fatalf("committed empty translation replayed: calls=%v err=%v", e.attempts, err)
	}
}
func TestFirstResponseHomeReleaseWaitHonorsTotalBudget(t *testing.T) {
	for _, waitAt := range []time.Duration{25 * time.Second, 30 * time.Second} {
		t.Run(waitAt.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := cliproxyexecutor.NewFirstResponseBudget(30*time.Second, 15*time.Second)
				ctx := cliproxyexecutor.WithFirstResponseBudget(context.Background(), b)
				_, a, err := b.StartAttempt(ctx)
				if err != nil {
					t.Fatal(err)
				}
				a.Close()
				time.Sleep(waitAt)
				reg := executionregistry.New()
				released := false
				ack := make(chan struct{})
				reg.SetReleaseSink(func(g executionregistry.ReleaseGroup, seq int64) *executionregistry.ReleaseTicket {
					released = true
					return executionregistry.NewReleaseTicket(g, seq, ack)
				})
				pending, err := reg.BeginDispatch()
				if err != nil {
					t.Fatal(err)
				}
				scope, err := reg.Install(pending, executionregistry.ScopeSpec{CredentialID: "test", Model: "budget-model", Accounted: true})
				if err != nil {
					t.Fatal(err)
				}
				selection := &HomeDispatchSelection{scope: scope}
				m := NewManager(nil, nil, nil)
				cfg := firstResponseConfig()
				cfg.CredentialConcurrency.CPACancelBound = 20 * time.Second
				m.SetConfig(cfg)
				start := time.Now()
				err = m.endHomeSelectionBeforeRedispatch(ctx, selection, "stream_start_failed")
				if elapsed := time.Since(start); elapsed != 30*time.Second-waitAt {
					t.Fatalf("release ACK exceeded remaining budget: %v", elapsed)
				}
				if !released || !cliproxyexecutor.IsFirstResponseTimeout(err) {
					t.Fatalf("release missing or errorwrong: released=%v err=%v", released, err)
				}
			})
		})
	}
}

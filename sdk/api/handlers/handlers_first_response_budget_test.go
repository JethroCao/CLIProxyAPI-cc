package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

type committedBootstrapExecutor struct{ bootstrapStreamExecutor }

func (*committedBootstrapExecutor) Identifier() string { return "codex" }
func TestFirstResponseCommittedDroppedPayloadCannotBootstrapRetry(t *testing.T) {
	upstream := &coreauth.Error{HTTPStatus: http.StatusServiceUnavailable, Message: "upstream failed after committed tool"}
	executor := &committedBootstrapExecutor{bootstrapStreamExecutor: bootstrapStreamExecutor{stream: func(ctx context.Context, _ int) (*coreexecutor.StreamResult, error) {
		b := coreexecutor.FirstResponseBudgetFromContext(ctx)
		_, a, err := b.StartAttempt(ctx)
		if err != nil {
			return nil, err
		}
		defer a.Close()
		if !a.Commit() {
			return nil, a.Err(ctx.Err())
		}
		ch := make(chan coreexecutor.StreamChunk, 2)
		ch <- coreexecutor.StreamChunk{Payload: []byte("drop")}
		ch <- coreexecutor.StreamChunk{Err: upstream}
		close(ch)
		return &coreexecutor.StreamResult{Chunks: ch}, nil
	}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{Codex: internalconfig.CodexConfig{FirstResponseTimeout: "30s"}})
	manager.RegisterExecutor(executor)
	id := "first-response-committed-handler"
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "first-response-committed-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	idRetry := "first-response-committed-handler-retry"
	registry.GetGlobalRegistry().RegisterClient(idRetry, "codex", []*registry.ModelInfo{{ID: "first-response-committed-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(idRetry) })
	if _, err := manager.Register(context.Background(), &coreauth.Auth{ID: idRetry, Provider: "codex", Status: coreauth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{BootstrapRetries: 2}}, manager)
	handler.SetPluginHost(&handlerInterceptorTestHost{interceptStreamChunk: func(_ context.Context, req pluginapi.StreamChunkInterceptRequest) pluginapi.StreamChunkInterceptResponse {
		return pluginapi.StreamChunkInterceptResponse{Body: cloneBytes(req.Body), DropChunk: string(req.Body) == "drop"}
	}})
	data, _, errs := handler.ExecuteStreamWithAuthManager(context.Background(), "openai", "first-response-committed-model", []byte(`{"model":"first-response-committed-model"}`), "")
	for range data {
	}
	var got error
	for msg := range errs {
		if msg != nil {
			got = msg.Error
		}
	}
	if executor.Calls() != 1 || !errors.Is(got, upstream) {
		t.Fatalf("committed dropped payload replayed or error replaced: calls=%d err=%v", executor.Calls(), got)
	}
}

package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func firstResponseTestConfig() *config.Config {
	return &config.Config{Codex: config.CodexConfig{
		FirstResponseTimeout: "30s", FirstResponseAttemptTimeout: "15s",
		// Releasing this old buffer at one second must not bypass the real deadline.
		StreamBootstrapBuffering: true, StreamBootstrapTimeout: "1s",
	}}
}

type firstResponseTestBody struct {
	io.ReadCloser
	stop func() bool
}

func (b *firstResponseTestBody) Close() error {
	b.stop()
	return b.ReadCloser.Close()
}

// Mock only the external HTTP boundary. Cancellation closes the pipe like the
// production transport; the real executor/scanner/finalizer still run.
func firstResponseTestTransport(produce func(context.Context, io.Writer)) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		stop := context.AfterFunc(req.Context(), func() { _ = writer.CloseWithError(req.Context().Err()) })
		go func() {
			defer writer.Close()
			produce(req.Context(), writer)
		}()
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}},
			Body: &firstResponseTestBody{ReadCloser: reader, stop: stop}, Request: req}, nil
	})
}

func firstResponseTestContext(rt http.RoundTripper) context.Context {
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", rt)
	return cliproxyexecutor.WithFirstResponseBudget(ctx, cliproxyexecutor.NewFirstResponseBudget(30*time.Second, 15*time.Second))
}

func TestCodexFirstResponseCancelsBeforeHeaders(t *testing.T) {
	usage.StartDefault(context.Background())
	synctest.Test(t, func(t *testing.T) {
		ctx := firstResponseTestContext(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}))
		start := time.Now()
		req, opts := codexTestRequest()
		result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
		if result != nil || !cliproxyexecutor.IsFirstResponseTimeout(err) {
			t.Fatalf("result=%v error=%v, want local timeout without response", result, err)
		}
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Fatalf("wait=%v, want 15s", elapsed)
		}
		if ctx.Err() != nil {
			t.Fatal("attempt canceled the caller context")
		}
	})
}

func TestCodexFirstResponseCancelsSilentOrHeartbeatBody(t *testing.T) {
	usage.StartDefault(context.Background())
	for _, prefix := range []string{"", "data: {\"partial\":", "data: " + codexCreatedEvent + "\n\n"} {
		t.Run(fmt.Sprintf("prefix_bytes_%d", len(prefix)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := firstResponseTestContext(firstResponseTestTransport(func(ctx context.Context, w io.Writer) {
					_, _ = io.WriteString(w, prefix)
					time.Sleep(2 * time.Second) // Virtual time inside synctest.
					if strings.HasSuffix(prefix, "\n\n") {
						_, _ = io.WriteString(w, "data: "+codexKeepaliveEvent+"\n\n")
					}
					<-ctx.Done()
				}))
				start := time.Now()
				req, opts := codexTestRequest()
				result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
				if result != nil || !cliproxyexecutor.IsFirstResponseTimeout(err) {
					t.Fatalf("result=%v error=%v, want timeout before any handshake escapes", result, err)
				}
				if time.Since(start) != 15*time.Second {
					t.Fatalf("wait=%v", time.Since(start))
				}
			})
		})
	}
}

func TestCodexFirstResponseProgressPreservesLongStream(t *testing.T) {
	usage.StartDefault(context.Background())
	for _, event := range []string{codexOutputDeltaEvent,
		`{"type":"response.output_item.added","item":{"type":"web_search_call","status":"in_progress"}}`,
		`{"type":"future.server.operation","status":"in_progress"}`} {
		t.Run(event, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := firstResponseTestContext(firstResponseTestTransport(func(ctx context.Context, w io.Writer) {
					_, _ = io.WriteString(w, "data: "+codexCreatedEvent+"\n\n")
					time.Sleep(10 * time.Second)
					_, _ = io.WriteString(w, "data: "+event+"\n\n")
					time.Sleep(40 * time.Second)
					if ctx.Err() != nil {
						t.Errorf("healthy stream canceled after progress: %v", ctx.Err())
						return
					}
					_, _ = io.WriteString(w, "data: "+codexCompletedEventBody+"\n\n")
				}))
				req, opts := codexTestRequest()
				result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
				if err != nil {
					t.Fatal(err)
				}
				output, err := drainChunks(result)
				if err != nil || !strings.Contains(output, "response.completed") {
					t.Fatalf("stream did not finish: %v %s", err, output)
				}
			})
		})
	}
}

func TestCodexFirstResponseExemptsHighReasoningAndLargeFinalPayload(t *testing.T) {
	usage.StartDefault(context.Background())
	for _, payload := range []string{
		`{"model":"gpt-5.6-terra","input":"hello","reasoning":{"effort":"high"}}`,
		`{"model":"gpt-5.6-terra","input":"` + strings.Repeat("x", 270000) + `"}`,
	} {
		t.Run(fmt.Sprintf("payload_bytes_%d", len(payload)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := firstResponseTestContext(firstResponseTestTransport(func(ctx context.Context, w io.Writer) {
					time.Sleep(45 * time.Second)
					if ctx.Err() != nil {
						t.Errorf("excluded request canceled: %v", ctx.Err())
						return
					}
					_, _ = io.WriteString(w, "data: "+codexCompletedEventBody+"\n\n")
				}))
				req, opts := codexTestRequest()
				req.Payload = []byte(payload)
				result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = drainChunks(result); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestCodexFirstResponseRejectsUnboundedPreambleWithoutLeakingHandshake(t *testing.T) {
	usage.StartDefault(context.Background())
	synctest.Test(t, func(t *testing.T) {
		ctx := firstResponseTestContext(firstResponseTestTransport(func(ctx context.Context, w io.Writer) {
			for range 60 {
				if _, err := io.WriteString(w, "data: "+codexKeepaliveEvent+"\n\n"); err != nil {
					return
				}
			}
			<-ctx.Done()
		}))
		req, opts := codexTestRequest()
		result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
		if result != nil || !cliproxyexecutor.IsFirstResponsePolicyError(err) || cliproxyexecutor.IsFirstResponseTimeout(err) {
			t.Fatalf("result=%v err=%v, want local buffer-limit failure without committed stream", result, err)
		}
		assertRequestScopedTestError(t, err)
	})
}

func TestCodexFirstResponsePreservesCallerCancellation(t *testing.T) {
	usage.StartDefault(context.Background())
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := context.WithValue(parent, "cliproxy.roundtripper", firstResponseTestTransport(func(ctx context.Context, _ io.Writer) { <-ctx.Done() }))
		ctx = cliproxyexecutor.WithFirstResponseBudget(ctx, cliproxyexecutor.NewFirstResponseBudget(30*time.Second, 15*time.Second))
		time.AfterFunc(time.Second, cancel)
		req, opts := codexTestRequest()
		result, err := NewCodexExecutor(firstResponseTestConfig()).ExecuteStream(ctx, codexTestAuth("http://codex.test"), req, opts)
		if result != nil || err != context.Canceled {
			t.Fatalf("result=%v error=%v, want original caller cancellation", result, err)
		}
	})
}

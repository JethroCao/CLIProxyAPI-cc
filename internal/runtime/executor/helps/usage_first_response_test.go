package helps

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type firstResponseUsageCapture struct {
	requestID string
	records   chan usage.Record
}

func (c *firstResponseUsageCapture) HandleUsage(_ context.Context, record usage.Record) {
	if record.RequestID == c.requestID {
		select {
		case c.records <- record:
		default:
		}
	}
}

func TestPublishFirstResponseTimeoutDoesNotPoisonHomeCredential(t *testing.T) {
	for _, withDetail := range []bool{false, true} {
		for _, tc := range []struct {
			name       string
			errs       []error
			wantStatus int
			neutral    bool
		}{
			{"attempt", []error{&cliproxyexecutor.FirstResponseTimeoutError{}}, http.StatusGatewayTimeout, true},
			{"final", []error{&cliproxyexecutor.FirstResponseTimeoutError{Final: true}}, http.StatusGatewayTimeout, true},
			{"wrapped", []error{nil, fmt.Errorf("execute: %w", &cliproxyexecutor.FirstResponseTimeoutError{})}, http.StatusGatewayTimeout, true},
			{"buffer limit", []error{&cliproxyexecutor.FirstResponseBufferLimitError{}}, http.StatusBadGateway, true},
			{"wrapped buffer limit", []error{fmt.Errorf("read: %w", &cliproxyexecutor.FirstResponseBufferLimitError{})}, http.StatusBadGateway, true},
			{"real upstream 502", []error{usageResponseBodyError{status: 502, message: "upstream bad gateway"}}, http.StatusBadGateway, false},
			{"real upstream 504", []error{usageResponseBodyError{status: 504, message: "upstream gateway timeout"}}, http.StatusGatewayTimeout, false},
			{"same text upstream 504", []error{usageResponseBodyError{status: 504, message: (&cliproxyexecutor.FirstResponseTimeoutError{}).Error()}}, http.StatusGatewayTimeout, false},
			{"caller cancelled", []error{context.Canceled, &cliproxyexecutor.FirstResponseTimeoutError{}}, clienterror.StatusClientClosedRequest, false},
			{"caller deadline", []error{context.DeadlineExceeded}, http.StatusGatewayTimeout, false},
		} {
			t.Run(fmt.Sprintf("%s/detail=%v", tc.name, withDetail), func(t *testing.T) {
				hook := setupResponseModelLoggerHook(t)
				ctx := usage.WithTraceID(context.Background(), "first-response-inbound-request")
				auth := &cliproxyauth.Auth{ID: "credential-first-response", Index: "credential-first-response-index", Provider: "codex", Metadata: map[string]any{"email": "budget@example.com"}}
				reporter := NewUsageReporter(ctx, "codex", "gpt-test", auth)
				capture := &firstResponseUsageCapture{requestID: reporter.RequestID(), records: make(chan usage.Record, 2)}
				usage.RegisterNamedPlugin("first-response-policy-usage", capture)
				if withDetail {
					reporter.PublishFailureWithDetail(ctx, usage.Detail{InputTokens: 7, OutputTokens: 2, TotalTokens: 9}, tc.errs...)
				} else {
					reporter.PublishFailure(ctx, tc.errs...)
				}
				var record usage.Record
				select {
				case record = <-capture.records:
				case <-time.After(3 * time.Second):
					t.Fatal("failure usage not delivered")
				}
				if !record.Failed || record.Fail.StatusCode != tc.wantStatus {
					t.Fatalf("failure outcome = %v/%d, want true/%d", record.Failed, record.Fail.StatusCode, tc.wantStatus)
				}
				wantIndex := "credential-first-response-index"
				if tc.neutral {
					wantIndex = ""
				}
				if record.AuthIndex != wantIndex {
					t.Fatalf("Home status attribution index = %q, want %q", record.AuthIndex, wantIndex)
				}
				if record.AuthID != "credential-first-response" || record.Source != "budget@example.com" || record.RequestID != reporter.RequestID() || record.TraceID != "first-response-inbound-request" {
					t.Fatal("compatibility handling lost account or request diagnostics")
				}
				if withDetail && record.Detail.TotalTokens != 9 {
					t.Fatalf("token detail lost: %+v", record.Detail)
				}
				if reporter.buildRecord(usage.Detail{}, false).AuthIndex != "credential-first-response-index" {
					t.Fatal("publishing a neutral failure mutated the reporter identity")
				}
				if tc.neutral {
					if record.Fail.Code == "" || !strings.Contains(record.Fail.Body, record.Fail.Code) {
						t.Fatalf("local failure classification lost: %+v", record.Fail)
					}
					found := false
					for _, entry := range hook.AllEntries() {
						if entry.Data["execution_id"] == record.RequestID && entry.Data["auth_index"] == "credential-first-response-index" && entry.Data["failure_code"] == record.Fail.Code {
							found = true
						}
					}
					if !found {
						t.Fatal("neutral failure lost credential index in structured diagnostics")
					}
				}
			})
		}
	}
}

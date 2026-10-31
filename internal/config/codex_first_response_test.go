package config

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestCodexFirstResponseBudgetDurations(t *testing.T) {
	for _, tc := range []struct {
		name, total, attempt   string
		wantTotal, wantAttempt time.Duration
	}{
		{name: "unset"},
		{name: "disabled total", total: "off", attempt: "15s"},
		{name: "invalid total", total: "later", attempt: "15s"},
		{name: "negative total", total: "-1s"},
		{name: "overflow seconds", total: "9223372036854775807"},
		{name: "thirty second budget", total: "30s", wantTotal: 30 * time.Second, wantAttempt: 15 * time.Second},
		{name: "numeric seconds", total: " 30 ", attempt: "10", wantTotal: 30 * time.Second, wantAttempt: 10 * time.Second},
		{name: "explicit split", total: "30s", attempt: "20s", wantTotal: 30 * time.Second, wantAttempt: 20 * time.Second},
		{name: "attempt capped by total", total: "30s", attempt: "1m", wantTotal: 30 * time.Second, wantAttempt: 30 * time.Second},
		{name: "invalid attempt uses half", total: "30s", attempt: "later", wantTotal: 30 * time.Second, wantAttempt: 15 * time.Second},
		{name: "zero attempt uses half", total: "30s", attempt: "0", wantTotal: 30 * time.Second, wantAttempt: 15 * time.Second},
		{name: "negative attempt uses half", total: "30s", attempt: "-1s", wantTotal: 30 * time.Second, wantAttempt: 15 * time.Second},
		{name: "smallest positive budget", total: "1ns", wantTotal: time.Nanosecond, wantAttempt: time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &CodexConfig{FirstResponseTimeout: tc.total, FirstResponseAttemptTimeout: tc.attempt}
			if got := cfg.FirstResponseTimeoutDuration(); got != tc.wantTotal {
				t.Fatalf("total duration = %v, want %v", got, tc.wantTotal)
			}
			if got := cfg.FirstResponseAttemptTimeoutDuration(); got != tc.wantAttempt {
				t.Fatalf("attempt duration = %v, want %v", got, tc.wantAttempt)
			}
		})
	}
	var cfg *CodexConfig
	if cfg.FirstResponseTimeoutDuration() != 0 || cfg.FirstResponseAttemptTimeoutDuration() != 0 {
		t.Fatal("nil config must leave first-response limits disabled")
	}
}

func TestCodexFirstResponseBudgetRequestLimit(t *testing.T) {
	for _, tc := range []struct{ configured, want int }{
		{0, 262144}, {-1, 262144}, {65536, 65536},
	} {
		cfg := &CodexConfig{FirstResponseMaxRequestBytes: tc.configured}
		if got := cfg.FirstResponseRequestByteLimit(); got != tc.want {
			t.Fatalf("request limit(%d) = %d, want %d", tc.configured, got, tc.want)
		}
	}
	var cfg *CodexConfig
	if cfg.FirstResponseRequestByteLimit() != 262144 {
		t.Fatal("nil config lost default request limit")
	}
}

func TestCodexFirstResponseBudgetConfigRoundTrip(t *testing.T) {
	const fields = "{first-response-timeout: 30s, first-response-attempt-timeout: 15s, first-response-max-request-bytes: 65536}"
	for name, raw := range map[string]string{
		"legacy Home":      "codex: " + fields + "\n",
		"upstream":         "upstream:\n  codex: " + fields + "\n",
		"historical OAuth": "oauth:\n  providers:\n    codex: " + fields + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := ParseConfigBytes([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			assertBudget := func(got *Config) {
				t.Helper()
				if got.Codex.FirstResponseTimeoutDuration() != 30*time.Second || got.Codex.FirstResponseAttemptTimeoutDuration() != 15*time.Second || got.Codex.FirstResponseRequestByteLimit() != 65536 {
					t.Fatalf("first-response policy did not survive config boundary: %+v", got.Codex)
				}
			}
			assertBudget(cfg)
			assertBudget(cfg.CloneForRuntime())
			assertBudget(cfg.ForAPIKey())
			migrated, _, err := NormalizeConfigLayout([]byte(raw), true)
			if err != nil {
				t.Fatal(err)
			}
			if err = ValidateV8Config(migrated); err != nil {
				t.Fatal(err)
			}
			var doc yaml.Node
			if err = yaml.Unmarshal(migrated, &doc); err != nil {
				t.Fatal(err)
			}
			if yamlPath(doc.Content[0], "upstream.codex.first-response-timeout") == nil {
				t.Fatalf("canonical YAML missing first-response policy: %s", migrated)
			}
			yamlBytes, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, encoded := range [][]byte{migrated, yamlBytes} {
				restored, err := ParseConfigBytes(encoded)
				if err != nil {
					t.Fatal(err)
				}
				assertBudget(restored)
			}
			wire, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var restored Config
			if err = json.Unmarshal(wire, &restored); err != nil {
				t.Fatal(err)
			}
			assertBudget(&restored)
		})
	}
}

func TestCodexFirstResponseCanonicalPolicyWins(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`codex: {first-response-timeout: 30s}
oauth: {providers: {codex: {first-response-timeout: 40s}}}
upstream: {codex: {first-response-timeout: "0"}}
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Codex.FirstResponseTimeoutDuration(); got != 0 {
		t.Fatalf("explicit canonical disable was overridden: %v", got)
	}
}

func TestCodexFirstResponseEnvironmentOverrides(t *testing.T) {
	for _, tc := range []struct {
		name, total, attempt, maxBytes string
		wantTotal, wantAttempt         time.Duration
		wantBytes                      int
	}{
		{"all overrides", "30s", "15s", "262144", 30 * time.Second, 15 * time.Second, 262144},
		{"total clamps YAML attempt", "30s", "", "", 30 * time.Second, 30 * time.Second, 131072},
		{"attempt only", "", "10s", "", 60 * time.Second, 10 * time.Second, 131072},
		{"bytes only", "", "", "65536", 60 * time.Second, 40 * time.Second, 65536},
		{"empty values use YAML", "", "", "", 60 * time.Second, 40 * time.Second, 131072},
		{"blank values use YAML", "  ", "\t", " ", 60 * time.Second, 40 * time.Second, 131072},
		{"explicit disable", "0", "15s", "", 0, 0, 131072},
		{"invalid total disables", "later", "15s", "", 0, 0, 131072},
		{"invalid attempt uses half", "", "later", "", 60 * time.Second, 30 * time.Second, 131072},
		{"zero attempt uses half", "", "0", "", 60 * time.Second, 30 * time.Second, 131072},
		{"invalid bytes use default", "", "", "later", 60 * time.Second, 40 * time.Second, 262144},
		{"zero bytes use default", "", "", "0", 60 * time.Second, 40 * time.Second, 262144},
		{"negative bytes use default", "", "", "-1", 60 * time.Second, 40 * time.Second, 262144},
		{"overflow bytes use default", "", "", "9223372036854775808", 60 * time.Second, 40 * time.Second, 262144},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CPA_CODEX_FIRST_RESPONSE_TIMEOUT", tc.total)
			t.Setenv("CPA_CODEX_FIRST_RESPONSE_ATTEMPT_TIMEOUT", tc.attempt)
			t.Setenv("CPA_CODEX_FIRST_RESPONSE_MAX_REQUEST_BYTES", tc.maxBytes)
			cfg := &CodexConfig{FirstResponseTimeout: "60s", FirstResponseAttemptTimeout: "40s", FirstResponseMaxRequestBytes: 131072}
			if total, attempt, limit := cfg.FirstResponseTimeoutDuration(), cfg.FirstResponseAttemptTimeoutDuration(), cfg.FirstResponseRequestByteLimit(); total != tc.wantTotal || attempt != tc.wantAttempt || limit != tc.wantBytes {
				t.Fatalf("effective policy = (%v,%v,%d), want (%v,%v,%d)", total, attempt, limit, tc.wantTotal, tc.wantAttempt, tc.wantBytes)
			}
			if cfg.FirstResponseTimeout != "60s" || cfg.FirstResponseAttemptTimeout != "40s" || cfg.FirstResponseMaxRequestBytes != 131072 {
				t.Fatal("environment overrides mutated persisted configuration")
			}
		})
	}
}

func TestCodexFirstResponseEnvironmentSurvivesHomeSnapshotReload(t *testing.T) {
	t.Setenv("CPA_CODEX_FIRST_RESPONSE_TIMEOUT", "30s")
	t.Setenv("CPA_CODEX_FIRST_RESPONSE_ATTEMPT_TIMEOUT", "15s")
	t.Setenv("CPA_CODEX_FIRST_RESPONSE_MAX_REQUEST_BYTES", "65536")
	for _, raw := range []string{"{}", "codex: {stream-bootstrap-buffering: true}\n"} {
		cfg, err := ParseConfigBytes([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Codex.FirstResponseTimeoutDuration() != 30*time.Second || cfg.Codex.FirstResponseAttemptTimeoutDuration() != 15*time.Second || cfg.Codex.FirstResponseRequestByteLimit() != 65536 {
			t.Fatal("Home snapshot without new fields lost node-local policy")
		}
		if cfg.Codex.FirstResponseTimeout != "" {
			t.Fatal("environment policy leaked into raw Home configuration")
		}
	}
	var absent *CodexConfig
	if absent.FirstResponseTimeoutDuration() != 0 || absent.FirstResponseAttemptTimeoutDuration() != 0 || absent.FirstResponseRequestByteLimit() != 262144 {
		t.Fatal("environment policy changed nil-config behavior")
	}
}

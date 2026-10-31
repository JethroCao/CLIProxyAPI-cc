package config

import (
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// FirstResponseTimeoutDuration returns the opt-in total pre-response wait budget.
// A nonempty CPA_CODEX_FIRST_RESPONSE_TIMEOUT overrides YAML without changing persisted config.
// Duration strings and integer seconds are accepted; non-positive or invalid values disable it.
func (c *CodexConfig) FirstResponseTimeoutDuration() time.Duration {
	if c == nil {
		return 0
	}
	return positiveCodexBudgetDuration(codexBudgetEnvironmentValue("CPA_CODEX_FIRST_RESPONSE_TIMEOUT", c.FirstResponseTimeout))
}

// FirstResponseAttemptTimeoutDuration returns an attempt's share of the total budget.
// A nonempty CPA_CODEX_FIRST_RESPONSE_ATTEMPT_TIMEOUT overrides YAML.
// Unset, invalid, and non-positive values use half the total; no attempt exceeds the total.
func (c *CodexConfig) FirstResponseAttemptTimeoutDuration() time.Duration {
	total := c.FirstResponseTimeoutDuration()
	if total <= 0 {
		return 0
	}
	attempt := positiveCodexBudgetDuration(codexBudgetEnvironmentValue("CPA_CODEX_FIRST_RESPONSE_ATTEMPT_TIMEOUT", c.FirstResponseAttemptTimeout))
	if attempt <= 0 {
		attempt = total / 2
		if attempt == 0 {
			attempt = total
		}
	}
	return min(attempt, total)
}

// FirstResponseRequestByteLimit returns the maximum finalized JSON size eligible for the policy.
// A nonempty CPA_CODEX_FIRST_RESPONSE_MAX_REQUEST_BYTES overrides YAML; invalid values use 256 KiB.
func (c *CodexConfig) FirstResponseRequestByteLimit() int {
	if c == nil {
		return 262144
	}
	limit := c.FirstResponseMaxRequestBytes
	if raw := strings.TrimSpace(os.Getenv("CPA_CODEX_FIRST_RESPONSE_MAX_REQUEST_BYTES")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 262144
		}
		limit = parsed
	}
	if limit <= 0 {
		return 262144
	}
	return limit
}

func codexBudgetEnvironmentValue(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func positiveCodexBudgetDuration(value string) time.Duration {
	value = strings.TrimSpace(value)
	if duration, err := time.ParseDuration(value); err == nil && duration > 0 {
		return duration
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 || seconds > math.MaxInt64/int64(time.Second) {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

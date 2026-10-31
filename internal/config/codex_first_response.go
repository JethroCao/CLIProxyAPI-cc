package config

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// FirstResponseTimeoutDuration returns the opt-in total pre-response wait budget.
// Duration strings and integer seconds are accepted; non-positive or invalid values disable it.
func (c *CodexConfig) FirstResponseTimeoutDuration() time.Duration {
	if c == nil {
		return 0
	}
	return positiveCodexBudgetDuration(c.FirstResponseTimeout)
}

// FirstResponseAttemptTimeoutDuration returns an attempt's share of the total budget.
// Unset, invalid, and non-positive values use half the total; no attempt exceeds the total.
func (c *CodexConfig) FirstResponseAttemptTimeoutDuration() time.Duration {
	total := c.FirstResponseTimeoutDuration()
	if total <= 0 {
		return 0
	}
	attempt := positiveCodexBudgetDuration(c.FirstResponseAttemptTimeout)
	if attempt <= 0 {
		attempt = total / 2
		if attempt == 0 {
			attempt = total
		}
	}
	return min(attempt, total)
}

// FirstResponseRequestByteLimit returns the maximum finalized JSON size eligible for the policy.
func (c *CodexConfig) FirstResponseRequestByteLimit() int {
	if c == nil || c.FirstResponseMaxRequestBytes <= 0 {
		return 262144
	}
	return c.FirstResponseMaxRequestBytes
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

# Codex First-Response Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Bound ordinary Codex HTTP streaming pre-response waiting to 30 seconds across at most two credential attempts.

**Architecture:** A request-scoped budget in the executor SDK survives conductor retry rounds. The Codex HTTP executor activates cancellable attempt contexts after final payload validation and atomically disarms on substantive progress. Existing Home selection lifecycles release attempts, without misclassifying local timeout as an unhealthy credential.

**Tech Stack:** Go 1.26, context cancellation, stoppable timers, existing SSE scanner and Home dispatch.

**Spec:** `docs/superpowers/specs/2026-10-31-first-response-budget-design.md`

## Global Constraints

- Opt-in defaults; ordinary deployment policy total 30s, attempt 15s, at most two attempts, finalized request <=262144 bytes.
- Exempt deep/unknown reasoning, large requests, WS, non-streaming, compact/images, and non-Codex executors.
- Preserve payload-finalizer barrier and caller cancellation; never log payloads or credentials.
- No output replay after progress, no healthy-stream lifetime timeout, no credential cooldown for local deadlines.

## Review Focus

- Heartbeats/partial SSE lines and a silent body must not keep a pending request alive indefinitely.
- Timer/progress races must produce one outcome and leave no active cancellation after successful progress.
- Home retry overrides, stale affinity, and bootstrap retries must not extend the shared budget or leak slots.
- Payload rules/model suffixes may change reasoning: eligibility uses finalized payload.
- Caller disconnect, excluded providers/WS, memory limits, and long post-first-event streams preserve their contracts.

## Task 1: Configuration

Files: `internal/config/config_types.go`, config tests, `config.example.yaml`, `AGENTS.md`.

- [x] RED: config parsing/normalization tests for disabled, 30s/15s, invalid values, nested/legacy config and byte threshold.
- [x] Add Codex FirstResponseTimeout, FirstResponseAttemptTimeout, FirstResponseMaxRequestBytes plus duration/limit methods.
- [x] GREEN: `go test ./internal/config`; document opt-in settings and narrow timeout exception.

## Task 2: Shared deadline and conductor lifecycle

Files: `sdk/cliproxy/executor/first_response_budget.go`, tests, conductor files/tests, handler bootstrap retry guard if necessary.

- [x] RED: use virtual time to prove shared total, per-attempt deadline, at most two attempts, atomic commit, caller cancel and cleanup.
- [x] Implement context-carried budget/attempt types; integrate manager creation and request-boundary exhaustion handling.
- [x] RED/GREEN: Home timeout switches account, releases execution before retry, and skips account cooldown; no extra outer retries.
- [x] Run `go test ./sdk/cliproxy/executor ./sdk/cliproxy/auth ./sdk/api/handlers`.

## Task 3: Codex streaming integration

Files: `internal/runtime/executor/helps/codex_first_response.go`, eligibility tests, `codex_executor_stream.go`, executor regression tests.

- [x] RED: stalled headers/body and heartbeat-only streams cancel; success/tool progress survives longer than budget; finalized high reasoning/large payload bypass.
- [x] Start attempt after finalization, keep safe bootstrap held, map local cancellation, terminate bounded buffers safely, cancel timer on progress and clean up on all exits.
- [x] GREEN: targeted executor/helper tests, then race tests.
- [x] Home v1.1.7 compatibility: typed local first-response policy errors retain failed usage (504 deadline, 502 buffer bound) and source diagnostics but omit scheduler AuthIndex on the record copy; regression-test plain/detail publishers, wrapped local errors, real upstream 502/504, caller cancellation, and structured credential diagnostics.

## Task 4: Verify and deliver

- [x] Independent review of all changed files against the spec and review focus; fix substantive findings with regression tests.
- [x] `gofmt` changed Go files; `go test ./...`; `go build -o /private/tmp/cpa-first-response-server ./cmd/server`.
- [x] Report exact scope, tests, deployment config and deployment state. Do not claim the live service changed without deployment verification.

## Execution ledger

- Isolated worktree: `/Users/caochenxu/Work/open-source/CLIProxyAPI-cc-first-response`, branch `feat/first-response-budget`, base `491b915a`.
- Native worktree tool rejected nested repository sourceRoot; manual git worktree used within writable workspace.
- Default Go cache write blocked by sandbox; use `GOCACHE=/private/tmp/cpa-first-response-go-cache GOMAXPROCS=4` for checks.
- User approved option 2 and 30s total; proceed without another design approval. No server deployment performed in this implementation task yet.

- Configuration and eligibility tests passed after observed RED failures for missing feature and configuration-update precedence.
- Executor regression tests passed for stalled headers, silent/partial/heartbeat SSE, 50-second streams after progress, server tools, unknown events, large/high-effort exemptions, buffer limits and caller cancellation.
- SDK/conductor tests passed for shared total, two-attempt cap, Home release before retry, bounded retry selection and release ACK, committed-response replay prevention, and preserved genuine upstream-failure accounting.
- Independent review found replay after untranslated progress, configuration-update precedence, and unbounded Home release ACK; each received a failing regression followed by a passing fix. Mixed-provider routes bypass the policy and non-Codex selections disable it before preparation.
- Initial broad runs exposed an existing WebRTC DataChannel timeout (also reproduced on main) and a Home client-close timing failure. Subsequent low-parallelism runs passed both; final whole-suite verification is recorded separately below.
- Deployment preparation is limited to read-only source comparison. Multiple relevant live files differ from fork base; no live binary, config, account or service changed.
- Initial Home selection cannot be interrupted by this policy until final payload eligibility is known. Its elapsed time is counted; once activated, retry selection, preparation and release waits share the remaining deadline.

- Final verification: `GOCACHE=/private/tmp/cpa-first-response-go-cache GOMAXPROCS=4 go test -p 2 ./...` exited 0; targeted `-race` tests across SDK/auth/handlers/executor/helpers exited 0, including the final replay/accounting fixes. Final `go build -o /private/tmp/cpa-first-response-server ./cmd/server` exited 0.

# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-10-07

First release. `agent-swarm-orchestrator` schedules work across a pool of
agents: it matches tasks to agents on a weighted score, enforces a per-tick
budget, resolves resource conflicts with named locks, preempts lower-priority
work when a higher-priority task needs the slot, retries failures with
exponential backoff, and records every mutation in a hash-chained audit ledger.
Zero runtime dependencies.

### Added

- **Weighted matching** (`src/matcher.ts`) — `score = 40 * coverage + 25 * headroom + 20 * reliability + 15 * fairness`: capability coverage first, then the less-loaded agent, then proven reliability, then a fairness ramp for the agent that has waited longest. Ties break by agent id, so a fixed clock gives a deterministic allocation.
- **Tick budget** (`src/scheduler.ts`) — each tick has `budgetPerTick` points and a task costs its `weight`. Work is allocated highest-effective-priority first, an unaffordable task is deferred with `budget-exhausted`, and a task heavier than the whole budget is rejected at submission.
- **Preemption** (`src/scheduler.ts`) — a queued task that outranks a running one by `preemptMargin` takes the slot; the victim goes back to `queued` with its attempt count and preemption count preserved, and releases its lock.
- **Retry with exponential backoff** (`src/scheduler.ts`, `src/store.ts`) — `backoffBaseMs * 2^attempts`, capped at `backoffCapMs`, and the task is not eligible again before `nextEligibleAt`; a task that exhausts `maxRetries` lands in `failed`.
- **Lock-based conflict resolution** (`src/locks.ts`) — a task declares a named lock, the scheduler will not run two holders of the same lock at once, and a blocked task is deferred with `lock-held-by:<holder>` and queued on that lock.
- **Hash-chained audit ledger** (`src/audit.ts`) — append-only SHA-256 chain from a genesis hash; `audit verify` reports a tampered or broken link and names the entry it broke at.
- **Atomic JSON persistence** (`src/persist.ts`) — state is written to a temp file and renamed, so a crash mid-write leaves the previous snapshot intact.
- **CLI** (`src/cli.ts`) — `demo`, `register`, `submit`, `list`, `tasks`, `tick`, `complete`, `fail`, `locks`, `stats`, `audit`, `audit verify` and `serve`, shipped as the `agent-swarm-orchestrator` bin, with exit codes 0 (success), 1 (validation, not found, conflict) and 2 (usage). Mutating and listing commands take `--state <file>`.
- **HTTP API** (`src/server.ts`) — `/stats`, `/agents`, `/tasks`, `/tick`, `/locks`, `/audit`, `/audit/verify` and `/save`; writes require the bearer token when the server is started with one.
- **Tests** (`tests/index.test.ts`) — 31 tests and 82 assertions over matching, budget, preemption, backoff, locks, audit and persistence, run with `bun test`.
- **CI** (`.github/workflows/ci.yml`) — typecheck, tests, build and a CLI demo smoke test on every push and pull request.

### Install

```bash
npm install github:Retsumdk/agent-swarm-orchestrator#v1.0.0
```

Bun users: `bun add github:Retsumdk/agent-swarm-orchestrator#v1.0.0`, then
`bun pm trust agent-swarm-orchestrator`, because Bun blocks a git dependency's
lifecycle scripts by default and `prepare` is what compiles `dist/`.

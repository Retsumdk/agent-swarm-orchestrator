# agent-swarm-orchestrator

Dynamic multi-agent swarm orchestration with greedy weighted scheduling, resource-aware allocation, lock-based conflict resolution, and retry with exponential backoff.

Zero runtime dependencies. Pure TypeScript, strict mode, Node 20+/Bun.

## The problem

Teams running fleets of AI agents hit the same coordination failures:

- **Nobody schedules.** Every agent grabs whatever it sees, hot agents melt down while idle agents starve, and one expensive task starves out ten cheap ones.
- **Nobody arbitrates.** Two agents mutate the same resource concurrently and the corruption is found days later, with no record of who did what.
- **Nobody retries with discipline.** A failed task is re-run immediately in a tight loop (or abandoned silently), and there is no durable answer to "how many times did we try this?"

Writing an ad-hoc queue per project means re-solving (and re-getting-wrong) allocation, fairness, preemption, locking, retries, and auditability every time.

## The solution

`agent-swarm-orchestrator` is a single embeddable scheduler that answers, in priority order, one question every tick: *which queued task should run on which agent right now?*

- **Greedy weighted scheduling** — each tick spends a finite budget on the highest effective-priority tasks first. Old tasks age upward (bounded), so a priority-0 task cannot starve behind a stream of priority-9 submissions.
- **Resource-aware allocation** — agents declare capabilities and a concurrency limit. Task→agent matching scores capability coverage (40%), free headroom (25%), reliability (20%), and time since the agent last received work (15%), so clusters spread load instead of hammering one agent.
- **Conflict resolution** — tasks can declare a named lock; while the lock is held, other claimants wait and are deferred with an explicit reason, not silently dropped.
- **Preemption** — a sufficiently higher-priority task can take a slot from a running *preemptible* task (the victim is requeued, its preemption count recorded).
- **Retries with exponential backoff** — a failed task re-enters the queue after `base·2^n` (capped) milliseconds, up to `maxRetries`; then it fails terminally with the reason recorded.
- **Tamper-evident audit** — every mutation lands in a hash-chained ledger (each entry hashes the previous one). `audit verify` proves nobody rewrote history.
- **Durable state** — the whole swarm (agents, tasks, locks, config, audit chain) round-trips through one JSON file written atomically (tmp + rename).

## How it works

```
                 ┌────────────────────────────────────────────┐
  submit() ────▶ │                 SwarmStore                 │
  register() ──▶ │  agents ─ tasks ─ LockTable ─ AuditLedger  │
                 └───────────────┬────────────────────────────┘
                                 │ tick()
                    ┌────────────▼────────────┐
                    │        runTick          │
                    │ 1. age eligible tasks   │  effective priority =
                    │ 2. order the queue      │  priority + min(cap, rate·waits)
                    │ 3. for each task:       │
                    │    lock held?  → defer  │  (waiter recorded)
                    │    budget left?→ else defer
                    │    rank agents  → best wins
                    │    nobody free? → preempt a lower-priority
                    │                    victim or defer with reason
                    └────────────┬────────────┘
                                 │ allocations + deferrals
              complete()/fail() ─┴─▶ retry w/ backoff or terminal failure
```

### Agent scoring

```
score = 40·coverage + 25·headroom + 20·reliability + 15·fairness

coverage    = |required ∩ agent.caps| / |required|     (1 when no caps required)
headroom    = 1 − active/maxConcurrency
reliability = completed / (completed + failed)          (0.5 when unproven)
fairness    = min(1, timeSinceLastAssignment / fairnessWindowMs)
```

Ties break by agent id, so runs are fully deterministic for a fixed clock.

### Tick budget

Each tick has `budgetPerTick` points. A task costs its `weight`. Work is allocated highest-effective-priority first; once the budget is spent, remaining tasks are deferred with `budget-exhausted`. A task heavier than the whole budget is rejected at submission — it could never be scheduled.

## Getting started

**Install from GitHub** (npm registry publish is not used by this project; git installs run the `prepare` script, which compiles `dist/`):

```bash
npm install github:Retsumdk/agent-swarm-orchestrator
```

(Bun users: `bun add github:Retsumdk/agent-swarm-orchestrator` then `bun pm trust agent-swarm-orchestrator` — Bun blocks git-dependency lifecycle scripts by default, so the package needs an explicit trust step before its `dist/` is built.)

**Or clone and build:**

```bash
git clone https://github.com/Retsumdk/agent-swarm-orchestrator.git
cd agent-swarm-orchestrator
bun install        # or: npm install
bun run typecheck  # strict tsc, zero errors
bun test           # 31 tests, 82 assertions
bun run build      # emits dist/ with declarations
```

## Library example

```ts
import { SwarmStore } from "agent-swarm-orchestrator";

const store = new SwarmStore();

store.registerAgent({ id: "reader", capabilities: ["read"], maxConcurrency: 2 });
store.registerAgent({ id: "writer", capabilities: ["write"], maxConcurrency: 1 });

store.submitTask({ id: "t-fetch", required: ["read"], priority: 1 });
store.submitTask({ id: "t-publish", required: ["write"], weight: 2 });

const result = store.tick();
// result.allocations: [{ taskId: "t-fetch", agentId: "reader", ... },
//                      { taskId: "t-publish", agentId: "writer", ... }]

store.complete("t-fetch", "fetched 3 pages");
store.stats();        // counts, capacity utilization, success rate, avg wait
store.auditTail(5);   // last 5 hash-chained audit entries
store.verifyAuditChain(); // { ok: true, brokenAt: null, entries: 6 }
```

## CLI

Every state-mutating command accepts `--state <file>` to persist the swarm between invocations.

```bash
# one process per command, shared durable state
node dist/cli.js --state /tmp/swarm.json register --agent reader --caps read --concurrency 2 --cost 1
node dist/cli.js --state /tmp/swarm.json register --agent writer --caps write --concurrency 1 --cost 2
node dist/cli.js --state /tmp/swarm.json submit --task t-fetch --required read --priority 1
node dist/cli.js --state /tmp/swarm.json submit --task t-publish --required write --weight 2
node dist/cli.js --state /tmp/swarm.json tick
node dist/cli.js --state /tmp/swarm.json complete --task t-fetch --outcome "fetched 3 pages"
node dist/cli.js --state /tmp/swarm.json stats
node dist/cli.js --state /tmp/swarm.json audit --limit 3
node dist/cli.js --state /tmp/swarm.json audit verify
node dist/cli.js --state /tmp/swarm.json serve --port 8080 --token $SWARM_TOKEN
```

Real captured session:

```
registered agent reader caps=[read] concurrency=2 cost=1
registered agent writer caps=[write] concurrency=1 cost=2
submitted task t-fetch state=queued priority=1 weight=1
submitted task t-publish state=queued priority=0 weight=2
allocated t-fetch -> reader
allocated t-publish -> writer
budget 3/10 | running=2
completed t-fetch agent=reader attempts=1
agents          2
tasks total     2
queued          0
running         1
done            1
failed          0
capacity        1/3 active (33%)
success rate    100% of terminal tasks
budget last tick 3/10
preemptions     0
avg wait        101ms
3 2026-10-06T20:10:46.913Z task.submitted {"id":"t-publish","required":["write"],"priority":0,"weight":2,"lock":null,"maxRetries":0,"preemptible":true}
4 2026-10-06T20:10:46.980Z tick.completed {"allocated":[{"taskId":"t-fetch","agentId":"reader"},{"taskId":"t-publish","agentId":"writer"}],"deferred":[],"preempted":[],"budgetUsed":3}
5 2026-10-06T20:10:47.048Z task.completed {"id":"t-fetch","agentId":"reader","attempts":1,"outcome":"fetched 3 pages"}
audit chain OK
```

Exit codes: `0` success, `1` error (validation / not-found / conflict), `2` usage error. Unknown flags and unknown commands are rejected — a mistyped option fails loudly instead of no-oping.

## The demo

`node dist/cli.js demo` walks the whole lifecycle deterministically (fixed clock): allocation, completion, a priority-10 preemption, a retry with backoff, and a final audit verification. Actual output:

```
submitted t-fetch, t-index
tick 1: allocated t-index->reader, t-fetch->reader
tick 1: deferred (none)
completed t-fetch (outcome: fetched 3 pages)
tick 2: allocated t-publish->writer
tick 3: allocated t-urgent->writer
tick 3: preempted t-publish
tick 4: allocated t-publish->writer
t-index failed once -> state=queued attempts=1
tick 5: allocated t-index->reader
final: done=4 failed=0 successRate=100% preempted=1
audit chain: OK (16 entries)
```

## HTTP API

`serve` exposes the store over `node:http`. With `--token` set, POST routes require `Authorization: Bearer <token>` (constant-time comparison); GET routes stay open.

| Route | Method | Body | Effect |
| --- | --- | --- | --- |
| `/healthz` | GET | — | liveness probe |
| `/stats` | GET | — | full `StatsSnapshot` |
| `/agents` | GET | — | all agents |
| `/agents` | POST | `{id, capabilities, maxConcurrency?, costPerTask?}` | register agent → `201` |
| `/tasks?filter=` | GET | — | tasks filtered by `all/queued/running/done/failed` |
| `/tasks` | POST | `{id?, required, priority?, weight?, lock?, maxRetries?, preemptible?, payload?}` | submit task → `201` |
| `/tasks/:id/complete` | POST | `{outcome?}` | mark running task done |
| `/tasks/:id/fail` | POST | `{reason?}` | fail task → retry w/ backoff or terminal |
| `/tick` | POST | — | run one scheduling tick |
| `/locks` | GET | — | lock holders and waiters |
| `/audit?limit=N` | GET | — | audit entries (whole chain when `limit` omitted) |
| `/audit/verify` | GET | — | hash-chain verification verdict |
| `/save` | POST | `{path}` | atomically persist state to a file |

Errors are structured: `{"error": "...", "code": "VALIDATION|NOT_FOUND|CONFLICT|CORRUPTION|CONFIG"}` with the matching HTTP status (400/404/409/500).

## Configuration

```ts
const store = new SwarmStore({
  now: () => fixedTime,          // inject a clock for deterministic tests
  scheduler: {
    budgetPerTick: 10,           // points spendable per tick
    agingRate: 0.5,              // effective-priority gain per wait
    agingCap: 5,                 // ...but never more than this
    preemptMargin: 2,            // how much higher a preemptor must rank
    backoffBaseMs: 1_000,        // retry n waits base·2^(n−1)
    backoffCapMs: 60_000,        // ...capped here
    fairnessWindowMs: 30_000,    // fairness ramp window
  },
});
```

## Project layout

```
src/
├── types.ts       # Task/Agent/Scheduler/Audit types + default config
├── errors.ts      # SwarmError hierarchy with stable codes + HTTP statuses
├── matcher.ts     # capability fit, agent scoring, queue ordering
├── scheduler.ts   # runTick: budget, deferral, preemption, backoff math
├── locks.ts       # named-lock table with waiter queues
├── audit.ts       # hash-chained ledger, tamper detection, torn-tail tolerance
├── persist.ts     # atomic JSON writes (tmp + rename), snapshot validation
├── store.ts       # SwarmStore: the lifecycle API + save/load
├── server.ts      # node:http API with bearer auth
├── cli.ts         # zero-dep CLI (node:util parseArgs)
└── index.ts       # public barrel
tests/index.test.ts # 31 tests across matcher, scheduler, locks, ledger, store, CLI
```

## License

MIT — see [LICENSE](./LICENSE).

#!/usr/bin/env node
import { parseArgs } from "node:util";
import { existsSync } from "node:fs";
import { SwarmStore } from "./store.js";
import type { TaskFilter } from "./store.js";
import { serve } from "./server.js";
import { SwarmError } from "./errors.js";

const USAGE = `agent-swarm-orchestrator — dynamic multi-agent swarm orchestration

Usage:
  agent-swarm-orchestrator demo
      Deterministic end-to-end demo: register agents, submit tasks, tick,
      complete/fail, retry-with-backoff, preempt, and print a final audit trail.

  agent-swarm-orchestrator register --agent <id> --caps <c1,c2> [--concurrency N] [--cost N]
  agent-swarm-orchestrator submit --task <id> --required <c1,c2> [--priority N] [--weight N]
                                   [--lock <name>] [--max-retries N] [--no-preempt] [--payload <text>]
  agent-swarm-orchestrator list --agent <id> [--filter queued|running|done|failed]
      (tasks assigned to an agent)
  agent-swarm-orchestrator tasks [--filter all|queued|running|done|failed]
  agent-swarm-orchestrator tick
  agent-swarm-orchestrator complete --task <id> [--outcome <text>]
  agent-swarm-orchestrator fail --task <id> [--reason <text>]
  agent-swarm-orchestrator locks
  agent-swarm-orchestrator stats
  agent-swarm-orchestrator audit [--limit N] | audit verify
  agent-swarm-orchestrator serve [--port N] [--token <secret>]
      HTTP API; writes (POST) require the token when --token is set.

All mutating/listing commands accept --state <file> to load and persist
swarm state as JSON (written atomically after each mutation).

Options:
  -h, --help          Show this help and exit.

Exit codes: 0 success, 1 error (validation/not found/conflict), 2 usage error.`;

interface GlobalOpts {
  state?: string | undefined;
}

const OPTIONS = {
  agent: { type: "string" },
  task: { type: "string" },
  caps: { type: "string" },
  required: { type: "string" },
  concurrency: { type: "string", short: "c" },
  cost: { type: "string" },
  priority: { type: "string", short: "p" },
  weight: { type: "string", short: "w" },
  lock: { type: "string", short: "l" },
  "max-retries": { type: "string" },
  preempt: { type: "boolean" },
  payload: { type: "string" },
  outcome: { type: "string", short: "o" },
  reason: { type: "string", short: "r" },
  filter: { type: "string", short: "f" },
  limit: { type: "string" },
  port: { type: "string" },
  token: { type: "string" },
  state: { type: "string", short: "s" },
} as const;

function run(argvIn?: readonly string[]): number {
  const rawArgs = [...(argvIn ?? process.argv.slice(2))];
  if (rawArgs.includes("-h") || rawArgs.includes("--help")) {
    console.log(USAGE);
    return 0;
  }
  let argv: string[];
  try {
    argv = parseArgs({ args: rawArgs, strict: true, allowPositionals: true, options: OPTIONS }).positionals;
  } catch (err) {
    console.error(`usage error: ${(err as Error).message}`);
    console.error("run 'agent-swarm-orchestrator --help' for usage");
    return 2;
  }

  if (argv.length === 0 || argv[0] === "-h" || argv[0] === "--help") {
    console.log(USAGE);
    return 0;
  }

  const [command, ...rest] = argv;

  if (command === "demo") {
    return runDemo();
  }

  const opts = parseArgs({ args: rawArgs, strict: true, allowPositionals: true, options: OPTIONS }).values;
  const global: GlobalOpts = {
    state: typeof opts.state === "string" ? opts.state : undefined,
  };

  try {
    switch (command) {
      case "register":
        return runRegister(opts, global);
      case "submit":
        return runSubmit(opts, global);
      case "list":
        return runList(opts, global);
      case "tasks":
        return runTasks(opts, global);
      case "tick":
        return runTick(global);
      case "complete":
        return runComplete(opts, global);
      case "fail":
        return runFail(opts, global);
      case "locks":
        return runLocks(global);
      case "stats":
        return runStats(global);
      case "audit":
        return runAudit(rest, opts, global);
      case "serve":
        return runServe(opts, global);
      default:
        console.error(`usage error: unknown command '${command}'`);
        console.error("run 'agent-swarm-orchestrator --help' for usage");
        return 2;
    }
  } catch (err) {
    if (err instanceof SwarmError) {
      console.error(`error: ${err.message} (${err.code})`);
      return 1;
    }
    console.error(`error: ${(err as Error).message}`);
    return 1;
  }
}

function loadStore(global: GlobalOpts): SwarmStore {
  if (global.state) {
    if (existsSync(global.state)) return SwarmStore.load(global.state);
    return new SwarmStore();
  }
  return new SwarmStore();
}

function persist(store: SwarmStore, global: GlobalOpts): void {
  if (global.state) store.save(global.state);
}

function runRegister(opts: Record<string, unknown>, global: GlobalOpts): number {
  const agentId = requireOpt(opts.agent, "--agent");
  const caps = requireOpt(opts.caps, "--caps");
  const store = loadStore(global);
  const agent = store.registerAgent({
    id: agentId,
    capabilities: caps.split(","),
    maxConcurrency: numOpt(opts.concurrency, 1),
    costPerTask: numOpt(opts.cost, 0),
  });
  console.log(
    `registered agent ${agent.id} caps=[${agent.capabilities.join(",")}] concurrency=${agent.maxConcurrency} cost=${agent.costPerTask}`,
  );
  persist(store, global);
  return 0;
}

function runSubmit(opts: Record<string, unknown>, global: GlobalOpts): number {
  const required = requireOpt(opts.required, "--required");
  const store = loadStore(global);
  const taskId = optStr(opts.task);
  const task = store.submitTask({
    ...(taskId !== undefined && { id: taskId }),
    required: required.split(","),
    priority: numOpt(opts.priority, 0),
    weight: numOpt(opts.weight, 1),
    lock: optStr(opts.lock) ?? null,
    maxRetries: numOpt(opts["max-retries"], 0),
    preemptible: opts.preempt !== false,
    payload: optStr(opts.payload) ?? "",
  });
  console.log(`submitted task ${task.id} state=${task.state} priority=${task.priority} weight=${task.weight}`);
  persist(store, global);
  return 0;
}

function runList(opts: Record<string, unknown>, global: GlobalOpts): number {
  const agentId = requireOpt(opts.agent, "--agent");
  const filter = parseFilter(opts.filter);
  const store = loadStore(global);
  store.agent(agentId);
  const tasks = store.tasksList(filter).filter((t) => t.assignedTo === agentId);
  printTasks(tasks);
  return 0;
}

function runTasks(opts: Record<string, unknown>, global: GlobalOpts): number {
  const store = loadStore(global);
  printTasks(store.tasksList(parseFilter(opts.filter)));
  return 0;
}

function runTick(global: GlobalOpts): number {
  const store = loadStore(global);
  const result = store.tick();
  for (const alloc of result.allocations) {
    console.log(`allocated ${alloc.taskId} -> ${alloc.agentId}${alloc.preempted ? " (preempted)" : ""}`);
  }
  for (const d of result.deferred) {
    console.log(`deferred ${d.taskId} (${d.reason})`);
  }
  console.log(
    `budget ${result.budgetUsed}/${store.config.budgetPerTick} | running=${store.stats().running}`,
  );
  persist(store, global);
  return 0;
}

function runComplete(opts: Record<string, unknown>, global: GlobalOpts): number {
  const taskId = requireOpt(opts.task, "--task");
  const store = loadStore(global);
  const task = store.complete(taskId, optStr(opts.outcome) ?? "");
  console.log(`completed ${task.id} agent=${task.assignedTo} attempts=${task.attempts}`);
  persist(store, global);
  return 0;
}

function runFail(opts: Record<string, unknown>, global: GlobalOpts): number {
  const taskId = requireOpt(opts.task, "--task");
  const store = loadStore(global);
  const task = store.fail(taskId, optStr(opts.reason) ?? "");
  if (task.state === "queued") {
    const wait = Math.max(0, task.nextEligibleAt - store.now);
    console.log(`retrying ${task.id} attempt=${task.attempts} next eligible in ${wait}ms`);
  } else {
    console.log(`failed ${task.id} permanently after ${task.attempts} attempts`);
  }
  persist(store, global);
  return 0;
}

function runLocks(global: GlobalOpts): number {
  const store = loadStore(global);
  const locks = store.lockStates();
  if (locks.length === 0) {
    console.log("no active locks");
    return 0;
  }
  for (const lock of locks) {
    console.log(`${lock.key} holder=${lock.holder ?? "-"} waiters=[${lock.waiters.join(",")}]`);
  }
  return 0;
}

function runStats(global: GlobalOpts): number {
  const s = loadStore(global).stats();
  console.log(`agents          ${s.agents}`);
  console.log(`tasks total     ${s.tasksTotal}`);
  console.log(`queued          ${s.queued}`);
  console.log(`running         ${s.running}`);
  console.log(`done            ${s.done}`);
  console.log(`failed          ${s.failed}`);
  console.log(`capacity        ${s.activeCapacity}/${s.totalCapacity} active (${(s.utilization * 100).toFixed(0)}%)`);
  console.log(`success rate    ${(s.successRate * 100).toFixed(0)}% of terminal tasks`);
  console.log(`budget last tick ${s.lastBudgetUsed}/${s.budgetPerTick}`);
  console.log(`preemptions     ${s.preemptedTotal}`);
  console.log(`avg wait        ${s.avgWaitMs === null ? "n/a" : `${Math.round(s.avgWaitMs)}ms`}`);
  return 0;
}

function runAudit(rest: string[], opts: Record<string, unknown>, global: GlobalOpts): number {
  const store = loadStore(global);
  if (rest.includes("verify")) {
    const verdict = store.verifyAuditChain();
    console.log(verdict.ok ? "audit chain OK" : `audit chain BROKEN at entry ${verdict.brokenAt}`);
    return verdict.ok ? 0 : 1;
  }
  const limit = numOpt(opts.limit, 0);
  const entries = limit > 0 ? store.auditTail(limit) : store.audit();
  for (const entry of entries) {
    console.log(`${entry.seq} ${new Date(entry.ts).toISOString()} ${entry.type} ${JSON.stringify(entry.payload)}`);
  }
  return 0;
}

function runServe(opts: Record<string, unknown>, global: GlobalOpts): number {
  const store = loadStore(global);
  const port = numOpt(opts.port, 8080);
  const token = optStr(opts.token);
  serve(store, { port, ...(token !== undefined ? { token } : {}) }).catch((err: Error) => {
    console.error(`error: ${err.message}`);
    process.exit(1);
  });
  return 0;
}

function runDemo(): number {
  const base = 1_700_000_000_000;
  let cursor = base;
  const store = new SwarmStore({ now: () => cursor });

  store.registerAgent({ id: "reader", capabilities: ["read"], maxConcurrency: 2, costPerTask: 1 });
  store.registerAgent({ id: "writer", capabilities: ["write"], maxConcurrency: 1, costPerTask: 2 });

  const t1 = store.submitTask({ id: "t-fetch", required: ["read"], priority: 1 });
  const t2 = store.submitTask({ id: "t-index", required: ["read"], priority: 2, maxRetries: 1 });
  console.log(`submitted ${t1.id}, ${t2.id}`);

  const r1 = store.tick();
  console.log(`tick 1: allocated ${fmtAllocs(r1)}`);
  console.log(`tick 1: deferred ${fmtDeferred(r1)}`);

  store.complete("t-fetch", "fetched 3 pages");
  console.log("completed t-fetch (outcome: fetched 3 pages)");

  store.submitTask({ id: "t-publish", required: ["write"], weight: 2 });
  const r2 = store.tick();
  console.log(`tick 2: allocated ${fmtAllocs(r2)}`);

  // Priority-10 work arrives while the only write-capable agent is busy.
  store.submitTask({ id: "t-urgent", required: ["write"], priority: 10 });
  const r3 = store.tick();
  console.log(`tick 3: allocated ${fmtAllocs(r3)}`);
  console.log(`tick 3: preempted ${r3.preempted.join(", ") || "(none)"}`);
  store.complete("t-urgent", "hotfix shipped");

  const r4 = store.tick();
  console.log(`tick 4: allocated ${fmtAllocs(r4)}`);
  store.complete("t-publish", "published digest");

  store.fail("t-index", "simulated crash");
  const requeued = store.task("t-index");
  console.log(`t-index failed once -> state=${requeued.state} attempts=${requeued.attempts}`);

  cursor += 2_000; // past the 1s exponential backoff
  const r5 = store.tick();
  console.log(`tick 5: allocated ${fmtAllocs(r5)}`);
  store.complete("t-index", "indexed 120 docs");

  const stats = store.stats();
  console.log(
    `final: done=${stats.done} failed=${stats.failed} successRate=${(stats.successRate * 100).toFixed(0)}% preempted=${stats.preemptedTotal}`,
  );

  const verify = store.verifyAuditChain();
  console.log(`audit chain: ${verify.ok ? "OK" : "BROKEN"} (${store.audit().length} entries)`);
  return 0;
}

function fmtAllocs(result: { allocations: { taskId: string; agentId: string; preempted: string | null }[] }): string {
  return result.allocations.map((a) => `${a.taskId}->${a.agentId}`).join(", ") || "(none)";
}

function fmtDeferred(result: { deferred: { taskId: string; reason: string }[] }): string {
  return result.deferred.map((d) => `${d.taskId}(${d.reason})`).join(", ") || "(none)";
}

// ---- helpers ----

function optStr(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

function numOpt(value: unknown, fallback: number): number {
  if (typeof value !== "string" || value === "") return fallback;
  const n = Number(value);
  if (!Number.isFinite(n)) {
    throw new SwarmError("VALIDATION", `expected a number, got '${value}'`);
  }
  return n;
}

function requireOpt(value: unknown, flag: string): string {
  const s = optStr(value);
  if (s === undefined) throw new SwarmError("VALIDATION", `missing required option ${flag}`);
  return s;
}

function parseFilter(value: unknown): TaskFilter {
  const filter = typeof value === "string" && value !== "" ? value : "all";
  const valid: TaskFilter[] = ["all", "queued", "running", "done", "failed"];
  if (!valid.includes(filter as TaskFilter)) {
    throw new SwarmError("VALIDATION", `invalid filter '${filter}'; expected one of ${valid.join(", ")}`);
  }
  return filter as TaskFilter;
}

function printTasks(tasks: ReturnType<SwarmStore["tasksList"]>): void {
  if (tasks.length === 0) {
    console.log("(no tasks)");
    return;
  }
  for (const t of tasks) {
    const parts = [
      t.id,
      `state=${t.state}`,
      `priority=${t.priority}`,
      `weight=${t.weight}`,
      `required=${t.required.join(",") || "-"}`,
      `agent=${t.assignedTo ?? "-"}`,
      `attempts=${t.attempts}`,
    ];
    if (t.lock !== null) parts.push(`lock=${t.lock}`);
    console.log(parts.join(" "));
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exit(run());
}

export { run, USAGE };

import { describe, test, expect, beforeEach } from "bun:test";
import { existsSync, writeFileSync } from "node:fs";
import {
  SwarmStore,
  SwarmError,
  AuditLedger,
  LockTable,
  backoffDelay,
  DEFAULT_SCHEDULER_CONFIG,
  stableStringify,
  type SchedulerConfig,
  type TaskRecord,
} from "../src/index.js";
import { orderQueue, effectivePriority, rankAgents } from "../src/matcher.js";
import { runTick } from "../src/scheduler.js";

function makeClock(start = 1_700_000_000_000): { now: () => number; advance: (ms: number) => void } {
  let cursor = start;
  return {
    now: () => cursor,
    advance: (ms) => {
      cursor += ms;
    },
  };
}

function seedStore(clock: ReturnType<typeof makeClock>): SwarmStore {
  const store = new SwarmStore({ now: clock.now });
  store.registerAgent({ id: "reader", capabilities: ["read"], maxConcurrency: 2, costPerTask: 1 });
  store.registerAgent({ id: "writer", capabilities: ["write"], maxConcurrency: 1, costPerTask: 2 });
  return store;
}

describe("stableStringify", () => {
  test("is key-order independent", () => {
    expect(stableStringify({ a: 1, b: 2 })).toBe(stableStringify({ b: 2, a: 1 }));
  });

  test("handles nested arrays and primitives", () => {
    expect(stableStringify({ list: [3, { z: 1, y: 2 }], n: null })).toBe(
      '{"list":[3,{"y":2,"z":1}],"n":null}',
    );
  });
});

describe("LockTable", () => {
  test("acquire, re-acquire by holder, release by non-holder", () => {
    const locks = new LockTable();
    expect(locks.acquire("db", "t1")).toBe(true);
    expect(locks.acquire("db", "t1")).toBe(true);
    expect(locks.acquire("db", "t2")).toBe(false);
    expect(locks.release("db", "t2")).toBe(false);
    expect(locks.release("db", "t1")).toBe(true);
    expect(locks.isHeld("db")).toBe(false);
  });

  test("rejects empty keys and tracks waiters", () => {
    const locks = new LockTable();
    expect(() => locks.acquire("", "t1")).toThrow(SwarmError);
    locks.acquire("gpu", "t1");
    locks.enqueue("gpu", "t2");
    locks.enqueue("gpu", "t2");
    expect([...locks.waitersOf("gpu")]).toEqual(["t2"]);
    locks.dequeue("gpu", "t2");
    expect(locks.waitersOf("gpu").length).toBe(0);
  });

  test("round-trips through JSON", () => {
    const locks = new LockTable();
    locks.acquire("db", "t1");
    locks.enqueue("db", "t2");
    const restored = LockTable.fromJSON(locks.snapshot());
    expect(restored.holderOf("db")).toBe("t1");
    expect([...restored.waitersOf("db")]).toEqual(["t2"]);
  });
});

describe("AuditLedger", () => {
  test("verify passes for an intact chain", () => {
    const ledger = new AuditLedger();
    ledger.append("a", { x: 1 }, 1);
    ledger.append("b", { y: 2 }, 2);
    const verdict = ledger.verify();
    expect(verdict.ok).toBe(true);
    expect(verdict.brokenAt).toBeNull();
    expect(verdict.entries).toBe(2);
  });

  test("detects a tampered middle entry", () => {
    const ledger = new AuditLedger();
    ledger.append("a", { x: 1 }, 1);
    ledger.append("b", { y: 2 }, 2);
    ledger.append("c", { z: 3 }, 3);
    const tampered = ledger.toJSON().map((e, i) =>
      i === 1 ? { ...e, payload: { y: 999 } } : e,
    );
    let failed = false;
    try {
      AuditLedger.fromJSON(tampered);
    } catch (err) {
      failed = err instanceof SwarmError && err.code === "CORRUPTION";
    }
    expect(failed).toBe(true);
  });

  test("tolerates a torn tail and keeps the intact prefix", () => {
    const ledger = new AuditLedger();
    ledger.append("a", { x: 1 }, 1);
    ledger.append("b", { y: 2 }, 2);
    const torn = ledger.toJSON().map((e, i) =>
      i === 1 ? { ...e, hash: "f".repeat(64) } : e,
    );
    const restored = AuditLedger.fromJSON(torn);
    expect(restored.length).toBe(1);
    expect(restored.verify().ok).toBe(true);
  });

  test("rejects a broken chain in the middle on load", () => {
    const ledger = new AuditLedger();
    ledger.append("a", { x: 1 }, 1);
    ledger.append("b", { y: 2 }, 2);
    ledger.append("c", { z: 3 }, 3);
    const broken = ledger.toJSON().map((e, i) =>
      i === 0 ? { ...e, prevHash: "deadbeef" } : e,
    );
    expect(() => AuditLedger.fromJSON(broken)).toThrow(SwarmError);
  });
});

describe("matcher", () => {
  const config: SchedulerConfig = { ...DEFAULT_SCHEDULER_CONFIG };

  function agent(id: string, caps: string[], maxConcurrency: number, active: number, completed: number, failed: number, lastAssignedAt: number | null) {
    return {
      id,
      capabilities: caps,
      maxConcurrency,
      costPerTask: 0,
      active,
      completed,
      failed,
      lastAssignedAt,
      registeredAt: 0,
    };
  }

  function task(id: string, required: string[], priority = 0, waits = 0): TaskRecord {
    return {
      id,
      required,
      priority,
      weight: 1,
      payload: "",
      lock: null,
      maxRetries: 0,
      preemptible: true,
      state: "queued",
      attempts: 0,
      waits,
      assignedTo: null,
      submittedAt: 0,
      nextEligibleAt: 0,
      startedAt: null,
      finishedAt: null,
      outcome: null,
      failureReason: null,
      preemptedCount: 0,
    };
  }

  test("scoreAgent prefers headroom, reliability, and fairness", () => {
    const ctx = { now: 30_000, fairnessWindowMs: 30_000 };
    const busy = agent("busy", ["read"], 2, 2, 10, 0, 0);
    const idle = agent("idle", ["read"], 2, 0, 0, 0, null);
    const t = task("t", ["read"]);
    const scores = rankAgents([busy, idle], t, ctx);
    expect(scores[0]!.agent.id).toBe("idle");
    expect(scores.length).toBe(1);
  });

  test("rankAgents drops agents at capacity and agents without capabilities", () => {
    const ctx = { now: 0, fairnessWindowMs: 30_000 };
    const full = agent("full", ["read"], 1, 1, 0, 0, null);
    const other = agent("other", ["write"], 1, 0, 0, 0, null);
    expect(rankAgents([full, other], task("t", ["read"]), ctx).length).toBe(0);
  });

  test("effectivePriority ages up to the cap", () => {
    const t = task("t", [], 1, 0);
    expect(effectivePriority(t, config)).toBe(1);
    t.waits = 4;
    expect(effectivePriority(t, config)).toBe(3);
    t.waits = 100;
    expect(effectivePriority(t, config)).toBe(1 + config.agingCap);
  });

  test("orderQueue breaks priority ties by submission time then id", () => {
    const a = task("a", [], 1);
    a.submittedAt = 10;
    const b = task("b", [], 1);
    b.submittedAt = 5;
    const c = task("c", [], 9);
    const ordered = orderQueue([a, b, c], config).map((t) => t.id);
    expect(ordered).toEqual(["c", "b", "a"]);
  });
});

describe("scheduler.runTick", () => {
  function setupAgents() {
    return [
      {
        id: "reader",
        capabilities: ["read"],
        maxConcurrency: 2,
        costPerTask: 0,
        active: 0,
        completed: 0,
        failed: 0,
        lastAssignedAt: null,
        registeredAt: 0,
      },
    ];
  }

  function queued(id: string, overrides: Partial<TaskRecord> = {}): TaskRecord {
    return {
      id,
      required: [],
      priority: 0,
      weight: 1,
      payload: "",
      lock: null,
      maxRetries: 0,
      preemptible: true,
      state: "queued",
      attempts: 0,
      waits: 0,
      assignedTo: null,
      submittedAt: 0,
      nextEligibleAt: 0,
      startedAt: null,
      finishedAt: null,
      outcome: null,
      failureReason: null,
      preemptedCount: 0,
      ...overrides,
    };
  }

  test("allocates greedily within the tick budget", () => {
    const agents = setupAgents();
    const tasks = [queued("a", { weight: 3 }), queued("b", { weight: 3 }), queued("c", { weight: 3 })];
    const result = runTick({
      agents,
      tasks,
      locks: new LockTable(),
      config: { ...DEFAULT_SCHEDULER_CONFIG, budgetPerTick: 7 },
      now: 0,
    });
    expect(result.allocations.map((a) => a.taskId)).toEqual(["a", "b"]);
    expect(result.budgetUsed).toBe(6);
    expect(result.deferred.map((d) => d.taskId)).toEqual(["c"]);
  });

  test("defers lock-held tasks and hands over after release", () => {
    const agents = setupAgents();
    const locks = new LockTable();
    const holder = queued("holder", { lock: "db", state: "running", assignedTo: "reader" });
    const waiter = queued("waiter", { lock: "db" });
    locks.acquire("db", "holder");
    const first = runTick({ agents, tasks: [holder, waiter], locks, config: DEFAULT_SCHEDULER_CONFIG, now: 0 });
    expect(first.deferred.map((d) => d.taskId)).toEqual(["waiter"]);
    expect(locks.waitersOf("db").includes("waiter")).toBe(true);
    locks.release("db", "holder");
    const second = runTick({ agents, tasks: [waiter], locks, config: DEFAULT_SCHEDULER_CONFIG, now: 1 });
    expect(second.allocations.map((a) => a.taskId)).toEqual(["waiter"]);
    expect(locks.holderOf("db")).toBe("waiter");
  });

  test("preempts a lower-priority running task when no agent has headroom", () => {
    const agents = setupAgents();
    agents[0]!.active = agents[0]!.maxConcurrency;
    const victim = queued("victim", { state: "running", assignedTo: "reader", priority: 1, startedAt: 0 });
    const urgent = queued("urgent", { priority: 10 });
    const result = runTick({
      agents,
      tasks: [victim, urgent],
      locks: new LockTable(),
      config: DEFAULT_SCHEDULER_CONFIG,
      now: 0,
    });
    expect(result.preempted).toEqual(["victim"]);
    expect(result.allocations).toEqual([
      { taskId: "urgent", agentId: "reader", weight: 1, preempted: "victim" },
    ]);
    expect(victim.state).toBe("queued");
    expect(victim.preemptedCount).toBe(1);
  });

  test("never preempts a non-preemptible task", () => {
    const agents = setupAgents();
    agents[0]!.active = agents[0]!.maxConcurrency;
    const victim = queued("victim", { state: "running", assignedTo: "reader", priority: 1, preemptible: false });
    const urgent = queued("urgent", { priority: 10 });
    const result = runTick({
      agents,
      tasks: [victim, urgent],
      locks: new LockTable(),
      config: DEFAULT_SCHEDULER_CONFIG,
      now: 0,
    });
    expect(result.preempted).toEqual([]);
    expect(result.allocations).toEqual([]);
    expect(result.deferred[0]?.reason).toBe("no-headroom");
  });

  test("reports no-capable-agent when no agent matches the requirements", () => {
    const result = runTick({
      agents: setupAgents(),
      tasks: [queued("t", { required: ["gpu"] })],
      locks: new LockTable(),
      config: DEFAULT_SCHEDULER_CONFIG,
      now: 0,
    });
    expect(result.deferred[0]?.reason).toBe("no-capable-agent");
  });

  test("backoffDelay grows exponentially and caps", () => {
    const config = { ...DEFAULT_SCHEDULER_CONFIG, backoffBaseMs: 1_000, backoffCapMs: 10_000 };
    expect(backoffDelay(1, config)).toBe(1_000);
    expect(backoffDelay(2, config)).toBe(2_000);
    expect(backoffDelay(3, config)).toBe(4_000);
    expect(backoffDelay(20, config)).toBe(10_000);
  });
});

describe("SwarmStore lifecycle", () => {
  let clock: ReturnType<typeof makeClock>;
  let store: SwarmStore;

  beforeEach(() => {
    clock = makeClock();
    store = seedStore(clock);
  });

  test("registerAgent validates duplicates, capabilities, and types", () => {
    expect(() => store.registerAgent({ id: "reader" })).toThrow(/already registered/);
    expect(() => store.registerAgent({ id: "x", capabilities: [] })).toThrow(/at least one capability/);
    expect(() => store.registerAgent({ id: "y", capabilities: ["r"], maxConcurrency: 0 })).toThrow(/integer >= 1/);
  });

  test("submitTask rejects duplicate ids and unschedulable weights", () => {
    store.submitTask({ id: "t1" });
    expect(() => store.submitTask({ id: "t1" })).toThrow(/already exists/);
    expect(() => store.submitTask({ id: "t2", weight: 99 })).toThrow(/exceeds budgetPerTick/);
  });

  test("auto-assigns sequential task ids", () => {
    const a = store.submitTask({});
    const b = store.submitTask({});
    expect(a.id).toBe("t-0001");
    expect(b.id).toBe("t-0002");
  });

  test("complete and fail enforce the running state", () => {
    store.submitTask({ id: "t1" });
    expect(() => store.complete("t1")).toThrow(/queued, not running/);
    store.tick();
    store.complete("t1", "ok");
    expect(store.task("t1").state).toBe("done");
    expect(() => store.fail("t1")).toThrow(/done, not running/);
  });

  test("retry with backoff, then permanent failure", () => {
    store.submitTask({ id: "t1", maxRetries: 1 });
    store.tick();
    store.fail("t1", "boom");
    expect(store.task("t1").state).toBe("queued");
    expect(store.task("t1").attempts).toBe(1);
    store.tick();
    expect(store.task("t1").state).toBe("queued");
    clock.advance(2_000);
    store.tick();
    expect(store.task("t1").state).toBe("running");
    store.fail("t1", "boom again");
    expect(store.task("t1").state).toBe("failed");
  });

  test("stats reflect the lifecycle", () => {
    store.submitTask({ id: "t1" });
    store.submitTask({ id: "t2" });
    store.tick();
    store.complete("t1");
    const s = store.stats();
    expect(s.done).toBe(1);
    expect(s.running).toBe(1);
    expect(s.utilization).toBeCloseTo(1 / 3);
    expect(s.successRate).toBe(1);
  });

  test("agent counters track completions and failures", () => {
    store.submitTask({ id: "t1" });
    store.tick();
    store.complete("t1");
    store.submitTask({ id: "t2", required: ["read"] });
    store.tick();
    store.fail("t2", "x");
    const reader = store.agent("reader");
    expect(reader.completed).toBe(1);
    expect(reader.failed).toBe(1);
  });
});

describe("SwarmStore persistence", () => {
  test("save/load round-trips agents, tasks, locks, and audit", () => {
    const clock = makeClock();
    const store = seedStore(clock);
    store.submitTask({ id: "t1", lock: "db" });
    store.tick();
    const tmp = `/tmp/swarm-test-${process.pid}-${Date.now()}.json`;
    store.save(tmp);
    const restored = SwarmStore.load(tmp, { now: clock.now });
    expect(restored.agent("reader").active).toBe(1);
    expect(restored.task("t1").state).toBe("running");
    expect(restored.task("t1").assignedTo).toBe("reader");
    expect(restored.lockStates().some((l) => l.key === "db" && l.holder === "t1")).toBe(true);
    expect(restored.verifyAuditChain().ok).toBe(true);
    expect(restored.audit().length).toBe(store.audit().length);
    expect(existsSync(tmp)).toBe(true);
  });

  test("load rejects corrupt files with CORRUPTION errors", () => {
    expect(() => SwarmStore.load("/nonexistent-path-xyz.json")).toThrow(/cannot read state file/);
    const bad = "/tmp/swarm-bad.json";
    writeFileSync(bad, "not json", "utf-8");
    expect(() => SwarmStore.load(bad)).toThrow(/not valid JSON/);
  });
});

describe("CLI", () => {
  test("demo runs end-to-end and reports a clean audit chain", async () => {
    const { run } = await import("../src/cli.js");
    const lines: string[] = [];
    const original = console.log;
    console.log = (msg: string) => {
      lines.push(msg);
    };
    try {
      const code = run(["demo"]);
      expect(code).toBe(0);
    } finally {
      console.log = original;
    }
    const text = lines.join("\n");
    expect(text).toContain("preempted=1");
    expect(text).toContain("successRate=100%");
    expect(text).toContain("audit chain: OK");
  });

  test("unknown commands are usage errors (exit 2)", async () => {
    const { run } = await import("../src/cli.js");
    expect(run(["bogus-command"])).toBe(2);
  });

  test("--help exits 0", async () => {
    const { run } = await import("../src/cli.js");
    expect(run(["--help"])).toBe(0);
  });
});

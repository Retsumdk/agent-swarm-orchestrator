import type {
  AgentSpec,
  AgentState,
  AuditEntry,
  LockSnapshot,
  SchedulerConfig,
  StatsSnapshot,
  StoreSnapshot,
  TaskRecord,
  TaskSpec,
  TickResult,
} from "./types.js";
import { DEFAULT_SCHEDULER_CONFIG } from "./types.js";
import { AuditLedger } from "./audit.js";
import { LockTable } from "./locks.js";
import { runTick, backoffDelay } from "./scheduler.js";
import { atomicWriteJSON, readJSON, isSnapshot } from "./persist.js";
import { validationError, notFoundError, conflictError, corruptionError } from "./errors.js";

export interface SwarmStoreOptions {
  now?: () => number;
  scheduler?: Partial<SchedulerConfig>;
}

export type TaskFilter = "all" | "queued" | "running" | "done" | "failed";

interface SavedState extends StoreSnapshot {
  audit?: AuditEntry[];
}

export class SwarmStore {
  private agents = new Map<string, AgentState>();
  private tasks = new Map<string, TaskRecord>();
  private locks = new LockTable();
  private ledger = new AuditLedger();
  private taskCounter = 0;
  private lastBudgetUsed = 0;
  private schedulerConfig: SchedulerConfig;
  private readonly clock: () => number;

  constructor(options: SwarmStoreOptions = {}) {
    this.clock = options.now ?? Date.now;
    this.schedulerConfig = { ...DEFAULT_SCHEDULER_CONFIG, ...options.scheduler };
  }

  get config(): SchedulerConfig {
    return this.schedulerConfig;
  }

  get now(): number {
    return this.clock();
  }

  // ---- agents ----

  registerAgent(spec: Partial<AgentSpec> & { id?: string }): AgentState {
    const id = requireString(spec.id, "agent id");
    if (this.agents.has(id)) {
      throw conflictError(`agent ${id} is already registered`);
    }
    const capabilities = normalizeCapabilities(spec.capabilities);
    const maxConcurrency = requireInt(spec.maxConcurrency ?? 1, "maxConcurrency", 1);
    const costPerTask = requireNumber(spec.costPerTask ?? 0, "costPerTask");
    if (capabilities.length === 0) {
      throw validationError("agent must declare at least one capability");
    }
    const agent: AgentState = {
      id,
      capabilities,
      maxConcurrency,
      costPerTask,
      active: 0,
      completed: 0,
      failed: 0,
      lastAssignedAt: null,
      registeredAt: this.now,
    };
    this.agents.set(id, agent);
    this.ledger.append("agent.registered", { id, capabilities, maxConcurrency, costPerTask }, this.now);
    return agent;
  }

  agent(id: string): AgentState {
    const agent = this.agents.get(id);
    if (!agent) throw notFoundError(`agent ${id} is not registered`);
    return agent;
  }

  agentsList(): AgentState[] {
    return [...this.agents.values()].sort((a, b) => a.id.localeCompare(b.id));
  }

  // ---- tasks ----

  submitTask(spec: Partial<TaskSpec> & { id?: string | undefined }): TaskRecord {
    let id: string;
    if (spec.id !== undefined && spec.id !== null) {
      id = requireString(spec.id, "task id");
      if (this.tasks.has(id)) throw conflictError(`task ${id} already exists`);
    } else {
      this.taskCounter += 1;
      id = `t-${String(this.taskCounter).padStart(4, "0")}`;
    }
    const required = normalizeCapabilities(spec.required);
    const priority = requireNumber(spec.priority ?? 0, "priority");
    const weight = requireInt(spec.weight ?? 1, "weight", 1);
    if (weight > this.schedulerConfig.budgetPerTick) {
      throw validationError(
        `task weight ${weight} exceeds budgetPerTick ${this.schedulerConfig.budgetPerTick} and could never be scheduled`,
      );
    }
    const payload = typeof spec.payload === "string" ? spec.payload : "";
    const lock = spec.lock !== undefined && spec.lock !== null
      ? requireString(spec.lock, "lock")
      : null;
    const maxRetries = requireInt(spec.maxRetries ?? 0, "maxRetries", 0);
    const preemptible = spec.preemptible === undefined ? true : spec.preemptible === true;
    const now = this.now;
    const task: TaskRecord = {
      id,
      required,
      priority,
      weight,
      payload,
      lock,
      maxRetries,
      preemptible,
      state: "queued",
      attempts: 0,
      waits: 0,
      assignedTo: null,
      submittedAt: now,
      nextEligibleAt: now,
      startedAt: null,
      finishedAt: null,
      outcome: null,
      failureReason: null,
      preemptedCount: 0,
    };
    this.tasks.set(id, task);
    this.ledger.append(
      "task.submitted",
      { id, required, priority, weight, lock, maxRetries, preemptible },
      now,
    );
    return task;
  }

  task(id: string): TaskRecord {
    const task = this.tasks.get(id);
    if (!task) throw notFoundError(`task ${id} does not exist`);
    return task;
  }

  tasksList(filter: TaskFilter = "all"): TaskRecord[] {
    const all = [...this.tasks.values()];
    const picked = filter === "all" ? all : all.filter((t) => t.state === filter);
    return picked.sort((a, b) => a.id.localeCompare(b.id));
  }

  // ---- lifecycle ----

  tick(): TickResult {
    const now = this.now;
    const result = runTick({
      agents: [...this.agents.values()],
      tasks: [...this.tasks.values()],
      locks: this.locks,
      config: this.schedulerConfig,
      now,
    });
    this.lastBudgetUsed = result.budgetUsed;
    const recordPayload = {
      allocated: result.allocations.map((a) => ({ taskId: a.taskId, agentId: a.agentId })),
      deferred: result.deferred,
      preempted: result.preempted,
      budgetUsed: result.budgetUsed,
    };
    this.ledger.append("tick.completed", recordPayload, now);
    return result;
  }

  complete(taskId: string, outcome: string = ""): TaskRecord {
    const task = this.task(taskId);
    if (task.state !== "running") {
      throw conflictError(`task ${taskId} is ${task.state}, not running`);
    }
    const agent = this.agent(task.assignedTo!);
    agent.active -= 1;
    agent.completed += 1;
    task.state = "done";
    task.finishedAt = this.now;
    task.outcome = outcome;
    if (task.lock !== null) {
      this.locks.release(task.lock, task.id);
      this.locks.dequeue(task.lock, task.id);
    }
    this.ledger.append(
      "task.completed",
      { id: task.id, agentId: agent.id, attempts: task.attempts, outcome },
      this.now,
    );
    return task;
  }

  fail(taskId: string, reason: string = ""): TaskRecord {
    const task = this.task(taskId);
    if (task.state !== "running") {
      throw conflictError(`task ${taskId} is ${task.state}, not running`);
    }
    const agent = this.agent(task.assignedTo!);
    agent.active -= 1;
    agent.failed += 1;
    if (task.lock !== null) {
      this.locks.release(task.lock, task.id);
      this.locks.dequeue(task.lock, task.id);
    }
    const now = this.now;
    if (task.attempts <= task.maxRetries) {
      task.state = "queued";
      task.assignedTo = null;
      task.startedAt = null;
      task.failureReason = reason;
      task.nextEligibleAt = now + backoffDelay(task.attempts, this.schedulerConfig);
      this.ledger.append(
        "task.retry",
        { id: task.id, attempts: task.attempts, maxRetries: task.maxRetries, nextEligibleAt: task.nextEligibleAt, reason },
        now,
      );
    } else {
      task.state = "failed";
      task.finishedAt = now;
      task.assignedTo = null;
      task.failureReason = reason;
      this.ledger.append(
        "task.failed",
        { id: task.id, attempts: task.attempts, reason },
        now,
      );
    }
    return task;
  }

  // ---- inspection ----

  lockStates(): LockSnapshot[] {
    return this.locks.snapshot();
  }

  stats(): StatsSnapshot {
    const all = [...this.tasks.values()];
    const byState = (s: TaskRecord["state"]) => all.filter((t) => t.state === s).length;
    const agents = this.agentsList();
    const totalCapacity = agents.reduce((sum, a) => sum + a.maxConcurrency, 0);
    const activeCapacity = agents.reduce((sum, a) => sum + a.active, 0);
    const terminal = all.filter((t) => t.state === "done" || t.state === "failed");
    const succeeded = all.filter((t) => t.state === "done").length;
    const started = all.filter((t) => t.startedAt !== null);
    const avgWaitMs = started.length === 0
      ? null
      : started.reduce((sum, t) => sum + (t.startedAt! - t.submittedAt), 0) / started.length;
    return {
      agents: agents.length,
      tasksTotal: all.length,
      queued: byState("queued"),
      running: byState("running"),
      done: byState("done"),
      failed: byState("failed"),
      activeCapacity,
      totalCapacity,
      utilization: totalCapacity === 0 ? 0 : activeCapacity / totalCapacity,
      successRate: terminal.length === 0 ? 0 : succeeded / terminal.length,
      budgetPerTick: this.schedulerConfig.budgetPerTick,
      lastBudgetUsed: this.lastBudgetUsed,
      avgWaitMs,
      preemptedTotal: all.reduce((sum, t) => sum + t.preemptedCount, 0),
    };
  }

  audit(): readonly AuditEntry[] {
    return this.ledger.all();
  }

  auditTail(n: number): readonly AuditEntry[] {
    return this.ledger.tail(n);
  }

  verifyAuditChain() {
    return this.ledger.verify();
  }

  // ---- persistence ----

  save(path: string): void {
    const snapshot: SavedState = {
      version: 1,
      agents: this.agentsList(),
      tasks: this.tasksList(),
      locks: this.locks.snapshot(),
      taskCounter: this.taskCounter,
      scheduler: this.schedulerConfig,
      lastBudgetUsed: this.lastBudgetUsed,
      audit: this.ledger.toJSON(),
    };
    atomicWriteJSON(path, snapshot);
  }

  static load(path: string, options: SwarmStoreOptions = {}): SwarmStore {
    const data = readJSON(path);
    if (!isSnapshot(data)) throw corruptionError(`state file ${path} is not a valid swarm snapshot`);
    const stored = data as SavedState;
    const store = new SwarmStore(options);
    if (stored.scheduler && typeof stored.scheduler === "object") {
      store.schedulerConfig = { ...store.schedulerConfig, ...stored.scheduler };
    }
    for (const raw of stored.agents) {
      if (raw === null || typeof raw !== "object") {
        throw corruptionError("agent record is not an object");
      }
      const a = raw as unknown as AgentState;
      if (typeof a.id !== "string" || a.id === "") throw corruptionError("agent record missing id");
      store.agents.set(a.id, a);
    }
    for (const raw of stored.tasks) {
      if (raw === null || typeof raw !== "object") {
        throw corruptionError("task record is not an object");
      }
      const t = raw as unknown as TaskRecord;
      if (typeof t.id !== "string" || t.id === "") throw corruptionError("task record missing id");
      store.tasks.set(t.id, t);
    }
    store.locks = LockTable.fromJSON(stored.locks);
    store.taskCounter = typeof stored.taskCounter === "number" ? stored.taskCounter : 0;
    store.lastBudgetUsed = typeof stored.lastBudgetUsed === "number" ? stored.lastBudgetUsed : 0;
    if (Array.isArray(stored.audit)) {
      store.ledger = AuditLedger.fromJSON(stored.audit);
    }
    return store;
  }
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw validationError(`${field} must be a non-empty string`);
  }
  return value;
}

function requireNumber(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw validationError(`${field} must be a finite number`);
  }
  return value;
}

function requireInt(value: unknown, field: string, min: number): number {
  const n = requireNumber(value, field);
  if (!Number.isInteger(n) || n < min) {
    throw validationError(`${field} must be an integer >= ${min}`);
  }
  return n;
}

function normalizeCapabilities(value: unknown): string[] {
  const raw = Array.isArray(value) ? value : typeof value === "string" ? value.split(",") : [];
  const seen = new Set<string>();
  for (const item of raw) {
    if (typeof item !== "string") continue;
    const trimmed = item.trim();
    if (trimmed !== "") seen.add(trimmed);
  }
  return [...seen];
}

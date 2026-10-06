export type TaskState = "queued" | "running" | "done" | "failed";

export interface AgentSpec {
  readonly id: string;
  readonly capabilities: readonly string[];
  readonly maxConcurrency: number;
  readonly costPerTask: number;
}

export interface AgentState extends AgentSpec {
  active: number;
  completed: number;
  failed: number;
  lastAssignedAt: number | null;
  registeredAt: number;
}

export interface TaskSpec {
  readonly id: string;
  readonly required: readonly string[];
  readonly priority: number;
  readonly weight: number;
  readonly payload: string;
  readonly lock: string | null;
  readonly maxRetries: number;
  readonly preemptible: boolean;
}

export interface TaskRecord extends TaskSpec {
  state: TaskState;
  attempts: number;
  waits: number;
  assignedTo: string | null;
  submittedAt: number;
  nextEligibleAt: number;
  startedAt: number | null;
  finishedAt: number | null;
  outcome: string | null;
  failureReason: string | null;
  preemptedCount: number;
}

export interface SchedulerConfig {
  budgetPerTick: number;
  agingRate: number;
  agingCap: number;
  preemptMargin: number;
  backoffBaseMs: number;
  backoffCapMs: number;
  fairnessWindowMs: number;
}

export const DEFAULT_SCHEDULER_CONFIG: SchedulerConfig = {
  budgetPerTick: 10,
  agingRate: 0.5,
  agingCap: 5,
  preemptMargin: 2,
  backoffBaseMs: 1_000,
  backoffCapMs: 60_000,
  fairnessWindowMs: 30_000,
};

export interface Allocation {
  taskId: string;
  agentId: string;
  weight: number;
  preempted: string | null;
}

export interface Deferral {
  taskId: string;
  reason: string;
}

export interface TickResult {
  allocations: Allocation[];
  deferred: Deferral[];
  budgetUsed: number;
  preempted: string[];
}

export interface LockSnapshot {
  key: string;
  holder: string | null;
  waiters: string[];
}

export interface AuditEntry {
  readonly seq: number;
  readonly ts: number;
  readonly type: string;
  readonly payload: Readonly<Record<string, unknown>>;
  readonly prevHash: string;
  readonly hash: string;
}

export interface StoreSnapshot {
  version: 1;
  agents: AgentState[];
  tasks: TaskRecord[];
  locks: LockSnapshot[];
  taskCounter: number;
  scheduler: SchedulerConfig;
  lastBudgetUsed: number;
}

export interface StatsSnapshot {
  agents: number;
  tasksTotal: number;
  queued: number;
  running: number;
  done: number;
  failed: number;
  activeCapacity: number;
  totalCapacity: number;
  utilization: number;
  successRate: number;
  budgetPerTick: number;
  lastBudgetUsed: number;
  avgWaitMs: number | null;
  preemptedTotal: number;
}

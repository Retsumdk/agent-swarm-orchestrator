import type {
  AgentState,
  Allocation,
  Deferral,
  SchedulerConfig,
  TaskRecord,
  TickResult,
} from "./types.js";
import { LockTable } from "./locks.js";
import { effectivePriority, hasCapabilities, orderQueue, rankAgents } from "./matcher.js";
import type { MatchContext } from "./matcher.js";

export function backoffDelay(attempts: number, config: SchedulerConfig): number {
  const shift = Math.max(0, attempts - 1);
  const raw = config.backoffBaseMs * Math.pow(2, Math.min(shift, 10));
  return Math.min(config.backoffCapMs, raw);
}

export interface TickInput {
  agents: AgentState[];
  tasks: TaskRecord[];
  locks: LockTable;
  config: SchedulerConfig;
  now: number;
}

export interface PreemptionDecision {
  victim: TaskRecord;
  agent: AgentState;
}

function findPreemptionTarget(
  task: TaskRecord,
  agents: readonly AgentState[],
  tasks: readonly TaskRecord[],
  config: SchedulerConfig,
): PreemptionDecision | null {
  const taskPriority = effectivePriority(task, config);
  let best: PreemptionDecision | null = null;
  let bestPriority = Number.POSITIVE_INFINITY;
  for (const running of tasks) {
    if (running.state !== "running" || !running.preemptible) continue;
    if (!running.assignedTo) continue;
    const agent = agents.find((a) => a.id === running.assignedTo);
    if (!agent) continue;
    // Preemption exists precisely to take a slot from a fully-loaded agent, so
    // the check here is capability fit only — requiring headroom would make
    // preemption unreachable (it only runs when rankAgents found nobody).
    if (!hasCapabilities(agent, task.required)) continue;
    const victimPriority = effectivePriority(running, config);
    if (taskPriority < victimPriority + config.preemptMargin) continue;
    if (victimPriority < bestPriority) {
      best = { victim: running, agent };
      bestPriority = victimPriority;
    }
  }
  return best;
}

export function runTick(input: TickInput): TickResult {
  const { agents, tasks, locks, config, now } = input;
  const allocations: Allocation[] = [];
  const deferred: Deferral[] = [];
  const preempted: string[] = [];
  let budget = config.budgetPerTick;

  const matchCtx: MatchContext = { now, fairnessWindowMs: config.fairnessWindowMs };
  const eligibleTasks = tasks.filter(
    (t) => t.state === "queued" && t.nextEligibleAt <= now,
  );
  for (const task of eligibleTasks) task.waits += 1;

  for (const task of orderQueue(eligibleTasks, config)) {
    if (task.lock !== null) {
      const holder = locks.holderOf(task.lock);
      if (holder !== null && holder !== task.id) {
        locks.enqueue(task.lock, task.id);
        deferred.push({ taskId: task.id, reason: `lock-held-by:${holder}` });
        continue;
      }
    }
    if (task.weight > budget) {
      deferred.push({ taskId: task.id, reason: "budget-exhausted" });
      continue;
    }

    const ranked = rankAgents(agents, task, matchCtx);
    let winner = ranked[0];
    let preemptTarget: PreemptionDecision | null = null;

    if (!winner) {
      preemptTarget = findPreemptionTarget(task, agents, tasks, config);
      if (preemptTarget) {
        const victim = preemptTarget.victim;
        victim.state = "queued";
        victim.assignedTo = null;
        victim.startedAt = null;
        victim.preemptedCount += 1;
        if (victim.lock !== null) locks.release(victim.lock, victim.id);
        preemptTarget.agent.active -= 1;
        preempted.push(victim.id);
        allocations.push({
          taskId: task.id,
          agentId: preemptTarget.agent.id,
          weight: task.weight,
          preempted: victim.id,
        });
        startTask(task, preemptTarget.agent, now, locks);
        budget -= task.weight;
        continue;
      }
      const capable = agents.filter((a) =>
        a.maxConcurrency > a.active &&
        task.required.every((cap) => a.capabilities.includes(cap)),
      );
      const reason = capable.length === 0
        ? task.required.length > 0 && !agents.some((a) => task.required.every((c) => a.capabilities.includes(c)))
          ? "no-capable-agent"
          : "no-headroom"
        : "no-headroom";
      deferred.push({ taskId: task.id, reason });
      continue;
    }

    allocations.push({
      taskId: task.id,
      agentId: winner.agent.id,
      weight: task.weight,
      preempted: null,
    });
    startTask(task, winner.agent, now, locks);
    budget -= task.weight;
  }

  return { allocations, deferred, budgetUsed: config.budgetPerTick - budget, preempted };
}

function startTask(task: TaskRecord, agent: AgentState, now: number, locks: LockTable): void {
  task.state = "running";
  task.assignedTo = agent.id;
  task.startedAt = now;
  task.attempts += 1;
  agent.active += 1;
  agent.lastAssignedAt = now;
  if (task.lock !== null) locks.acquire(task.lock, task.id);
}

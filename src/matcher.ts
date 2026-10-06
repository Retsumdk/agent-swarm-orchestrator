import type { AgentState, SchedulerConfig, TaskRecord } from "./types.js";

export function hasCapabilities(agent: AgentState, required: readonly string[]): boolean {
  const owned = new Set(agent.capabilities);
  return required.every((cap) => owned.has(cap));
}

export function isEligible(agent: AgentState, task: TaskRecord): boolean {
  if (agent.active >= agent.maxConcurrency) return false;
  return hasCapabilities(agent, task.required);
}

export interface MatchContext {
  now: number;
  fairnessWindowMs: number;
}

export interface AgentScore {
  agent: AgentState;
  score: number;
  coverage: number;
  headroom: number;
  reliability: number;
  fairness: number;
}

/**
 * Deterministic agent ranking for a task.
 *
 * score = 40*capabilityCoverage + 25*headroom + 20*reliability + 15*fairness
 * - coverage: fraction of required capabilities the agent holds (1 when eligible)
 * - headroom: 1 - active/maxConcurrency (prefer less-loaded agents)
 * - reliability: completed / (completed + failed), 0.5 when unproven
 * - fairness: 0..1 ramp over fairnessWindowMs since the agent last got work,
 *   so busy clusters spread load instead of hammering one agent.
 */
export function scoreAgent(agent: AgentState, task: TaskRecord, ctx: MatchContext): AgentScore {
  const required = task.required.length;
  const owned = new Set(agent.capabilities);
  const coverage = required === 0
    ? 1
    : task.required.filter((cap) => owned.has(cap)).length / required;
  const headroom = agent.maxConcurrency <= 0
    ? 0
    : Math.max(0, 1 - agent.active / agent.maxConcurrency);
  const attempted = agent.completed + agent.failed;
  const reliability = attempted === 0 ? 0.5 : agent.completed / attempted;
  const since = agent.lastAssignedAt === null
    ? ctx.fairnessWindowMs
    : Math.max(0, ctx.now - agent.lastAssignedAt);
  const fairness = Math.min(1, since / ctx.fairnessWindowMs);
  const score = 40 * coverage + 25 * headroom + 20 * reliability + 15 * fairness;
  return { agent, score, coverage, headroom, reliability, fairness };
}

export function rankAgents(
  agents: readonly AgentState[],
  task: TaskRecord,
  ctx: MatchContext,
): AgentScore[] {
  return agents
    .map((a) => scoreAgent(a, task, ctx))
    .filter((s) => isEligible(s.agent, task))
    .sort((a, b) => b.score - a.score || a.agent.id.localeCompare(b.agent.id));
}

export function effectivePriority(task: TaskRecord, config: SchedulerConfig): number {
  const aged = task.priority + config.agingRate * task.waits;
  return task.priority + Math.min(config.agingCap, aged - task.priority);
}

export function orderQueue(
  tasks: readonly TaskRecord[],
  config: SchedulerConfig,
): TaskRecord[] {
  return [...tasks].sort((a, b) => {
    const pa = effectivePriority(a, config);
    const pb = effectivePriority(b, config);
    if (pa !== pb) return pb - pa;
    if (a.submittedAt !== b.submittedAt) return a.submittedAt - b.submittedAt;
    return a.id.localeCompare(b.id);
  });
}

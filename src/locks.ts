import type { LockSnapshot } from "./types.js";
import { validationError } from "./errors.js";

export interface LockState {
  key: string;
  holder: string | null;
  waiters: string[];
}

export class LockTable {
  private locks = new Map<string, LockState>();

  private state(key: string): LockState {
    let state = this.locks.get(key);
    if (!state) {
      state = { key, holder: null, waiters: [] };
      this.locks.set(key, state);
    }
    return state;
  }

  isHeld(key: string): boolean {
    return this.locks.get(key)?.holder !== undefined &&
      this.locks.get(key)!.holder !== null;
  }

  holderOf(key: string): string | null {
    return this.locks.get(key)?.holder ?? null;
  }

  isHeldBy(key: string, taskId: string): boolean {
    return this.holderOf(key) === taskId;
  }

  waitersOf(key: string): readonly string[] {
    return this.locks.get(key)?.waiters ?? [];
  }

  enqueue(key: string, taskId: string): void {
    const state = this.state(key);
    if (state.holder === taskId) return;
    if (!state.waiters.includes(taskId)) state.waiters.push(taskId);
  }

  dequeue(key: string, taskId: string): void {
    const state = this.locks.get(key);
    if (!state) return;
    state.waiters = state.waiters.filter((id) => id !== taskId);
  }

  acquire(key: string, taskId: string): boolean {
    if (!key) throw validationError("lock key must be a non-empty string");
    const state = this.state(key);
    if (state.holder === taskId) return true;
    if (state.holder !== null) return false;
    state.holder = taskId;
    state.waiters = state.waiters.filter((id) => id !== taskId);
    return true;
  }

  release(key: string, taskId: string): boolean {
    const state = this.locks.get(key);
    if (!state || state.holder !== taskId) return false;
    state.holder = null;
    return true;
  }

  snapshot(): LockSnapshot[] {
    return [...this.locks.values()].map((s) => ({
      key: s.key,
      holder: s.holder,
      waiters: [...s.waiters],
    }));
  }

  static fromJSON(data: unknown): LockTable {
    const table = new LockTable();
    if (!Array.isArray(data)) return table;
    for (const raw of data) {
      if (raw === null || typeof raw !== "object") continue;
      const s = raw as Record<string, unknown>;
      if (typeof s.key !== "string" || s.key === "") continue;
      const state = table.state(s.key);
      state.holder = typeof s.holder === "string" ? s.holder : null;
      state.waiters = Array.isArray(s.waiters)
        ? s.waiters.filter((w): w is string => typeof w === "string")
        : [];
    }
    return table;
  }
}

import { readFileSync, writeFileSync, renameSync, unlinkSync, existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { corruptionError } from "./errors.js";
import type { StoreSnapshot } from "./types.js";

export function atomicWriteJSON(path: string, value: unknown): void {
  const dir = dirname(path);
  if (dir && !existsSync(dir)) mkdirSync(dir, { recursive: true });
  const tmp = `${path}.tmp-${process.pid}-${Date.now()}`;
  try {
    writeFileSync(tmp, JSON.stringify(value, null, 2) + "\n", "utf-8");
    renameSync(tmp, path);
  } catch (err) {
    try {
      if (existsSync(tmp)) unlinkSync(tmp);
    } catch {
      // best-effort cleanup
    }
    throw err;
  }
}

export function readJSON(path: string): unknown {
  let raw: string;
  try {
    raw = readFileSync(path, "utf-8");
  } catch (err) {
    throw corruptionError(`cannot read state file ${path}: ${(err as Error).message}`);
  }
  try {
    return JSON.parse(raw);
  } catch (err) {
    throw corruptionError(`state file ${path} is not valid JSON: ${(err as Error).message}`);
  }
}

export function isSnapshot(value: unknown): value is StoreSnapshot {
  if (value === null || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    v.version === 1 &&
    Array.isArray(v.agents) &&
    Array.isArray(v.tasks) &&
    Array.isArray(v.locks)
  );
}

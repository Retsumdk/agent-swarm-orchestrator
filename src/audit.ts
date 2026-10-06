import { createHash } from "node:crypto";
import type { AuditEntry } from "./types.js";
import { corruptionError } from "./errors.js";

const GENESIS_HASH = "0".repeat(64);

export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") {
    const raw = JSON.stringify(value);
    return raw === undefined ? "null" : raw;
  }
  if (Array.isArray(value)) {
    return "[" + value.map(stableStringify).join(",") + "]";
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return (
    "{" +
    keys.map((k) => JSON.stringify(k) + ":" + stableStringify(record[k])).join(",") +
    "}"
  );
}

function entryHash(entry: Omit<AuditEntry, "hash">): string {
  return createHash("sha256")
    .update(`${entry.prevHash}|${entry.seq}|${entry.ts}|${entry.type}|${stableStringify(entry.payload)}`)
    .digest("hex");
}

export interface ChainVerification {
  ok: boolean;
  brokenAt: number | null;
  entries: number;
}

export class AuditLedger {
  private entries: AuditEntry[] = [];

  get length(): number {
    return this.entries.length;
  }

  all(): readonly AuditEntry[] {
    return this.entries;
  }

  tail(n: number): readonly AuditEntry[] {
    if (n <= 0) return [];
    return this.entries.slice(Math.max(0, this.entries.length - n));
  }

  append(type: string, payload: Record<string, unknown>, ts: number): AuditEntry {
    const seq = this.entries.length;
    const prevHash = seq === 0 ? GENESIS_HASH : this.entries[seq - 1]!.hash;
    const unsigned = { seq, ts, type, payload, prevHash };
    const entry: AuditEntry = { ...unsigned, hash: entryHash(unsigned) };
    this.entries.push(entry);
    return entry;
  }

  verify(): ChainVerification {
    let prevHash = GENESIS_HASH;
    for (let i = 0; i < this.entries.length; i++) {
      const entry = this.entries[i]!;
      const { hash, ...unsigned } = entry;
      if (unsigned.prevHash !== prevHash || entryHash(unsigned) !== hash) {
        return { ok: false, brokenAt: i, entries: this.entries.length };
      }
      prevHash = hash;
    }
    return { ok: true, brokenAt: null, entries: this.entries.length };
  }

  toJSON(): AuditEntry[] {
    return [...this.entries];
  }

  static fromJSON(data: unknown): AuditLedger {
    if (!Array.isArray(data)) throw corruptionError("audit log is not an array");
    const entries: AuditEntry[] = data.map((raw, i) => {
      if (raw === null || typeof raw !== "object") {
        throw corruptionError(`audit entry ${i} is not an object`);
      }
      const e = raw as Record<string, unknown>;
      for (const field of ["seq", "ts", "type", "prevHash", "hash"]) {
        if (!(field in e)) throw corruptionError(`audit entry ${i} missing ${field}`);
      }
      return {
        seq: e.seq as number,
        ts: e.ts as number,
        type: e.type as string,
        payload: (e.payload ?? {}) as Record<string, unknown>,
        prevHash: e.prevHash as string,
        hash: e.hash as string,
      };
    });
    const ledger = new AuditLedger();
    ledger.entries = entries;
    const verdict = ledger.verify();
    if (!verdict.ok) {
      const tornTail = verdict.brokenAt === entries.length - 1;
      if (!tornTail) {
        throw corruptionError(`audit chain broken at entry ${verdict.brokenAt}`);
      }
      ledger.entries = entries.slice(0, verdict.brokenAt!);
    }
    return ledger;
  }
}

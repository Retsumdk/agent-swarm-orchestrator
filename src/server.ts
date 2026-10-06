import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { timingSafeEqual } from "node:crypto";
import { SwarmError, validationError } from "./errors.js";
import { SwarmStore } from "./store.js";
import type { TaskFilter } from "./store.js";

export interface ServeOptions {
  port: number;
  host?: string;
  token?: string;
}

function sendJSON(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

function authorized(req: IncomingMessage, token: string | undefined): boolean {
  if (!token) return true;
  const header = req.headers.authorization ?? "";
  if (!header.startsWith("Bearer ")) return false;
  const given = Buffer.from(header.slice(7));
  const expected = Buffer.from(token);
  if (given.length !== expected.length) return false;
  return timingSafeEqual(given, expected);
}

function parseBody(raw: string): Record<string, unknown> {
  if (raw.trim() === "") return {};
  try {
    const parsed: unknown = JSON.parse(raw);
    if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw validationError("request body must be a JSON object");
    }
    return parsed as Record<string, unknown>;
  } catch (err) {
    if (err instanceof SwarmError) throw err;
    throw validationError(`request body is not valid JSON: ${(err as Error).message}`);
  }
}

function optString(body: Record<string, unknown>, key: string): string | undefined {
  const v = body[key];
  return typeof v === "string" ? v : undefined;
}

function optNumber(body: Record<string, unknown>, key: string): number | undefined {
  const v = body[key];
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function optBoolean(body: Record<string, unknown>, key: string): boolean | undefined {
  const v = body[key];
  return typeof v === "boolean" ? v : undefined;
}

function optStringList(body: Record<string, unknown>, key: string): string[] | undefined {
  const v = body[key];
  if (typeof v === "string") {
    return v.split(",").map((part) => part.trim()).filter((part) => part !== "");
  }
  if (Array.isArray(v)) return v.filter((item): item is string => typeof item === "string");
  return undefined;
}

function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  return new Promise((resolve, reject) => {
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
    req.on("error", reject);
  });
}

const VALID_FILTERS: readonly TaskFilter[] = ["all", "queued", "running", "done", "failed"];

export function createSwarmServer(store: SwarmStore, options: ServeOptions): Server {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? "/", `http://localhost:${options.port}`);
    const path = url.pathname;
    const write = Boolean(req.method === "POST" || req.method === "PUT");

    try {
      if (path === "/healthz") {
        sendJSON(res, 200, { ok: true });
        return;
      }

      if (write && !authorized(req, options.token)) {
        sendJSON(res, 401, { error: "unauthorized: missing or invalid bearer token" });
        return;
      }

      if (path === "/stats" && req.method === "GET") {
        sendJSON(res, 200, store.stats());
        return;
      }

      if (path === "/agents" && req.method === "GET") {
        sendJSON(res, 200, { agents: store.agentsList() });
        return;
      }

      if (path === "/agents" && req.method === "POST") {
        const body = parseBody(await readBody(req));
        const id = optString(body, "id");
        const capabilities = optStringList(body, "capabilities") ?? optStringList(body, "caps");
        const maxConcurrency = optNumber(body, "maxConcurrency");
        const costPerTask = optNumber(body, "costPerTask");
        const agent = store.registerAgent({
          ...(id !== undefined && { id }),
          ...(capabilities !== undefined && { capabilities }),
          ...(maxConcurrency !== undefined && { maxConcurrency }),
          ...(costPerTask !== undefined && { costPerTask }),
        });
        sendJSON(res, 201, agent);
        return;
      }

      if (path === "/tasks" && req.method === "GET") {
        const filterParam = url.searchParams.get("filter") ?? "all";
        if (!VALID_FILTERS.includes(filterParam as TaskFilter)) {
          throw validationError(
            `invalid filter '${filterParam}'; expected one of ${VALID_FILTERS.join(", ")}`,
          );
        }
        sendJSON(res, 200, { tasks: store.tasksList(filterParam as TaskFilter) });
        return;
      }

      if (path === "/tasks" && req.method === "POST") {
        const body = parseBody(await readBody(req));
        const id = optString(body, "id");
        const required = optStringList(body, "required");
        const priority = optNumber(body, "priority");
        const weight = optNumber(body, "weight");
        const payload = optString(body, "payload");
        const lock = optString(body, "lock");
        const maxRetries = optNumber(body, "maxRetries");
        const preemptible = optBoolean(body, "preemptible");
        const task = store.submitTask({
          ...(id !== undefined && { id }),
          ...(required !== undefined && { required }),
          ...(priority !== undefined && { priority }),
          ...(weight !== undefined && { weight }),
          ...(payload !== undefined && { payload }),
          ...(lock !== undefined && { lock }),
          ...(maxRetries !== undefined && { maxRetries }),
          ...(preemptible !== undefined && { preemptible }),
        });
        sendJSON(res, 201, task);
        return;
      }

      const taskMatch = /^\/tasks\/([^/]+)\/(complete|fail)$/.exec(path);
      if (taskMatch && req.method === "POST") {
        const taskId = taskMatch[1]!;
        const action = taskMatch[2]!;
        const body = action === "complete" ? parseBody(await readBody(req)) : {};
        const task = action === "complete"
          ? store.complete(taskId, typeof body.outcome === "string" ? body.outcome : "")
          : store.fail(taskId, typeof body.reason === "string" ? body.reason : "");
        sendJSON(res, 200, task);
        return;
      }

      if (path === "/tick" && req.method === "POST") {
        sendJSON(res, 200, store.tick());
        return;
      }

      if (path === "/locks" && req.method === "GET") {
        sendJSON(res, 200, { locks: store.lockStates() });
        return;
      }

      if (path === "/audit" && req.method === "GET") {
        const n = Number(url.searchParams.get("limit") ?? "0");
        const entries = Number.isFinite(n) && n > 0 ? store.auditTail(Math.floor(n)) : store.audit();
        sendJSON(res, 200, { entries });
        return;
      }

      if (path === "/audit/verify" && req.method === "GET") {
        sendJSON(res, 200, store.verifyAuditChain());
        return;
      }

      if (path === "/save" && req.method === "POST") {
        const body = parseBody(await readBody(req));
        const path2 = typeof body.path === "string" && body.path !== "" ? body.path : null;
        if (!path2) throw validationError("body must include a 'path' string for the state file");
        store.save(path2);
        sendJSON(res, 200, { ok: true, path: path2 });
        return;
      }

      sendJSON(res, 404, { error: `no route for ${req.method} ${path}` });
    } catch (err) {
      if (err instanceof SwarmError) {
        sendJSON(res, err.httpStatus, { error: err.message, code: err.code });
        return;
      }
      sendJSON(res, 500, { error: `internal error: ${(err as Error).message}` });
    }
  });
  return server;
}

export function serve(store: SwarmStore, options: ServeOptions): Promise<void> {
  const server = createSwarmServer(store, options);
  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(options.port, options.host ?? "127.0.0.1", () => {
      console.log(`swarm API listening on http://localhost:${options.port}`);
    });
    const shutdown = () => {
      server.close(() => resolve());
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  });
}


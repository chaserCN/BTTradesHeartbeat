import crypto from "node:crypto";

import { rowToProbe } from "./heartbeat-storage.mjs";

export const SQL_ROW_LIMIT = 5_000;

export function createApiHandler(options) {
  const {
    apiToken,
    store,
    startedAtMs,
    getStatsContext,
  } = options;
  const now = options.now ?? Date.now;

  return async function handleApiRequest(request, response) {
    const url = new URL(request.url, "http://localhost");
    const route = url.pathname.replace(/\/+$/, "") || "/";

    if (route === "/health") {
      const last = store.getLastProbe();
      sendJson(response, 200, {
        ok: true,
        uptimeSeconds: Math.round((now() - startedAtMs) / 1000),
        lastProbeAt: last?.at ?? null,
        lastVerdict: last?.verdict ?? null,
      });
      return;
    }

    if (!isAuthorized(request, url, apiToken)) {
      sendJson(response, 401, { error: "unauthorized" });
      return;
    }

    if (route === "/api/stats") {
      const last = store.getLastProbe();
      const counts = store.getReadOnlyDb()
        .prepare("SELECT verdict, COUNT(*) AS count FROM probes GROUP BY verdict")
        .all();
      const span = store.getReadOnlyDb()
        .prepare("SELECT MIN(at) AS first, MAX(at) AS last, COUNT(*) AS total FROM probes")
        .get();
      const context = getStatsContext();
      sendJson(response, 200, {
        now: new Date(now()).toISOString(),
        uptimeSeconds: Math.round((now() - startedAtMs) / 1000),
        quietHours: context.quietHours,
        probeRunning: context.probeRunning,
        nextProbeAt: context.nextProbeAt,
        history: { total: span?.total ?? 0, first: span?.first ?? null, last: span?.last ?? null },
        verdictCounts: Object.fromEntries(counts.map((row) => [row.verdict, row.count])),
        lastProbe: last,
        config: context.config,
      });
      return;
    }

    const probeMatch = /^\/api\/probes\/(\d+)$/.exec(route);
    if (probeMatch) {
      const probe = store.withTrades(store.getProbeById(Number(probeMatch[1])));
      if (!probe) {
        sendJson(response, 404, { error: "probe not found" });
        return;
      }
      sendJson(response, 200, probe);
      return;
    }

    if (route === "/api/probes") {
      const limit = clampInt(url.searchParams.get("limit"), 100, 1, 2_000);
      const conditions = [];
      const parameters = [];
      const hours = url.searchParams.get("hours");
      const since = url.searchParams.get("since");
      if (hours) {
        conditions.push("at >= ?");
        parameters.push(new Date(now() - clampInt(hours, 24, 1, 24 * 365) * 3_600_000).toISOString());
      } else if (since) {
        conditions.push("at >= ?");
        parameters.push(since);
      }
      const verdict = url.searchParams.get("verdict");
      if (verdict) {
        conditions.push(`verdict IN (${verdict.split(",").map(() => "?").join(", ")})`);
        parameters.push(...verdict.split(","));
      }
      const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
      const rows = store.getReadOnlyDb()
        .prepare(`SELECT * FROM probes ${where} ORDER BY id DESC LIMIT ?`)
        .all(...parameters, limit)
        .map(rowToProbe);
      sendJson(response, 200, { count: rows.length, probes: rows });
      return;
    }

    if (route === "/api/sql") {
      const sql = url.searchParams.get("q") || (request.method === "POST" ? await readBody(request) : "");
      runReadOnlyQuery(response, sql, store.getReadOnlyDb(), now);
      return;
    }

    sendJson(response, 404, {
      error: "unknown route",
      routes: ["/health", "/api/stats", "/api/probes", "/api/probes/:id", "/api/sql?q=SELECT..."],
    });
  };
}

export function runReadOnlyQuery(response, rawSql, db, now = Date.now) {
  const sql = (rawSql || "").trim().replace(/;\s*$/, "");
  if (!sql) {
    sendJson(response, 400, { error: "missing query (?q= or POST body)" });
    return;
  }
  if (!/^(select|with)\b/i.test(sql)) {
    sendJson(response, 400, { error: "only SELECT/WITH queries are allowed" });
    return;
  }
  if (sql.includes(";")) {
    sendJson(response, 400, { error: "only a single statement is allowed" });
    return;
  }

  try {
    const startedMs = now();
    const rows = db.prepare(sql).all();
    sendJson(response, 200, {
      rowCount: rows.length,
      truncated: rows.length > SQL_ROW_LIMIT,
      elapsedMs: now() - startedMs,
      rows: rows.slice(0, SQL_ROW_LIMIT),
    });
  } catch (error) {
    sendJson(response, 400, { error: error.message });
  }
}

function isAuthorized(request, url, apiToken) {
  const header = request.headers.authorization || "";
  const provided = header.startsWith("Bearer ") ? header.slice(7) : url.searchParams.get("token") || "";
  const expected = Buffer.from(apiToken);
  const actual = Buffer.from(provided);
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";
    request.on("data", (chunk) => {
      body += chunk;
      if (body.length > 100_000) {
        reject(new Error("request body too large"));
        request.destroy();
      }
    });
    request.on("end", () => resolve(body));
    request.on("error", reject);
  });
}

export function sendJson(response, status, payload) {
  const body = JSON.stringify(payload, null, 2);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  });
  response.end(body);
}

function clampInt(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, Math.trunc(parsed)));
}

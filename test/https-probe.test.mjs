import assert from "node:assert/strict";
import test from "node:test";
import http from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createHeartbeatStore } from "../heartbeat-storage.mjs";
import { createApiHandler } from "../heartbeat-api.mjs";
import { DEFAULT_CHART_URL, httpsConfig, probeHttps, validateChart, createHttpsMonitor } from "../https-probe.mjs";

const nowMs = 1_790_000_000_000;
const chart = () => ({ code: 200, data: { info: { last: "80000", timestamp: nowMs / 1000 }, chart: [{ t: nowMs / 1000 - 180, p: "79990" }, { t: nowMs / 1000, p: 80000 }] } });
const result = (verdict = "ok", note = "ok") => ({ at: new Date(nowMs).toISOString(), verdict, note, durationMs: 20, points: 2 });
function fixture(t) {
  const dir = mkdtempSync(path.join(tmpdir(), "https-probe-"));
  const db = path.join(dir, "test.db");
  const store = createHeartbeatStore(db);
  t.after(() => { store.close(); rmSync(dir, { recursive: true, force: true }); });
  return { store, db };
}

test("configuration defaults to five minutes and rejects broken timer values", () => {
  assert.equal(httpsConfig({}).intervalMs, 300_000);
  for (const value of ["oops", "", "0", "-1", "Infinity"]) {
    assert.throws(() => httpsConfig({ HTTPS_INTERVAL_MINUTES: value }), /Invalid/);
  }
});

// Intent: an HTTP 200 error page or corrupt/stale payload must never be reported as a healthy chart.
test("chart validation rejects API errors, empty/corrupt prices and stale or future data", () => {
  assert.equal(validateChart(chart(), nowMs, 900_000).verdict, "ok");
  for (const [mutate, expected] of [
    [b => { b.code = 500; }, "api_error"],
    [b => { b.data.chart = []; }, "empty_chart"],
    [b => { b.data.info.last = null; }, "invalid_chart"],
    [b => { b.data.chart[0].p = true; }, "invalid_chart"],
    [b => { b.data.chart[0].p = ""; }, "invalid_chart"],
    [b => { b.data.chart[0].t = "NaN"; }, "invalid_chart"],
    [b => { b.data.info.timestamp -= 901; }, "stale_data"],
    [b => { b.data.chart.forEach(p => p.t -= 901); }, "stale_data"],
    [b => { b.data.info.timestamp += 301; }, "future_data"],
  ]) {
    const body = chart(); mutate(body);
    assert.equal(validateChart(body, nowMs, 900_000).note, expected);
  }
  const boundary = chart(); boundary.data.info.timestamp -= 900;
  assert.equal(validateChart(boundary, nowMs, 900_000).verdict, "ok");
  const rounded = chart(); rounded.data.chart[1].t += 180;
  assert.equal(validateChart(rounded, nowMs, 900_000).verdict, "ok");
});

test("request uses production HTTPS chart and reads complete body before timing", async () => {
  const times = [0, 6000];
  const value = await probeHttps({ now: () => nowMs, mono: () => times.shift(), fetchImpl: async (url, options) => {
    assert.equal(url, DEFAULT_CHART_URL);
    assert.equal(new URL(url).protocol, "https:");
    assert.equal(options.redirect, "manual");
    assert.equal(options.headers["Cache-Control"], "no-cache");
    return Response.json(chart());
  } });
  assert.equal(value.note, "slow");
  assert.equal(value.verdict, "degraded");
  assert.equal(value.durationMs, 6000);
  assert.equal(value.points, 2);
});

test("HTTP failures, redirects, HTML, huge bodies and TLS errors are failures", async () => {
  for (const [makeResponse, note] of [
    [() => new Response("bad gateway", { status: 502 }), "http_error"],
    [() => new Response("", { status: 301, headers: { location: "http://example.org" } }), "http_error"],
    [() => new Response("<html>login</html>"), "invalid_json"],
    [() => new Response("x".repeat(101)), "oversize"],
    [() => { throw new Error("certificate expired"); }, "network_error"],
  ]) {
    const actual = await probeHttps({ fetchImpl: async () => makeResponse(), maxBytes: 100 });
    assert.equal(actual.verdict, "down"); assert.equal(actual.note, note);
  }
});

// Intent: a server that sends headers then stalls must release the single-flight guard within the timeout.
test("timeout covers a stalled response body, not just response headers", async (t) => {
  const server = http.createServer((_req, res) => { res.writeHead(200); res.write('{"code":'); });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => { server.closeAllConnections(); server.close(); });
  const value = await probeHttps({ timeoutMs: 100, fetchImpl: (_url, options) => fetch(`http://127.0.0.1:${server.address().port}`, options) });
  assert.equal(value.note, "timeout");
  assert.equal(value.verdict, "down");
});

test("single flight prevents overlapping HTTPS checks and unlocks after exceptions", async (t) => {
  const { store } = fixture(t);
  let finish; let calls = 0;
  const monitor = createHttpsMonitor({ store, probe: () => { calls++; return new Promise(resolve => { finish = resolve; }); }, notify: async () => {}, isQuiet: () => false });
  const first = monitor.run();
  assert.equal(await monitor.run(), null);
  assert.equal(calls, 1);
  finish(result()); await first;
  assert.equal(monitor.running, false);
  assert.equal(store.getHttpsProbes().length, 1);
  const broken = createHttpsMonitor({ store, probe: async () => { throw new Error("failed"); }, notify: async () => {}, isQuiet: () => false });
  await assert.rejects(broken.run(), /failed/);
  assert.equal(broken.running, false);
});

// Intent: Telegram failures or restarts must not lose an outage/recovery notification.
test("notifications retry failed sends, suppress unchanged state, and report recovery", async (t) => {
  const { store } = fixture(t);
  let verdict = "down"; let attempts = 0; const messages = [];
  const options = { store, probe: async () => result(verdict, verdict === "ok" ? "ok" : "timeout"), isQuiet: () => false,
    notify: async m => { if (++attempts === 1) throw new Error("Telegram unavailable"); messages.push(m); } };
  const monitor = createHttpsMonitor(options);
  await assert.rejects(monitor.run(), /Telegram/);
  assert.equal(store.getHttpsProbes().length, 1);
  await monitor.run(); await monitor.run();
  assert.equal(messages.length, 1);
  const restarted = createHttpsMonitor(options);
  await restarted.run(); assert.equal(messages.length, 1);
  verdict = "ok"; await restarted.run();
  assert.equal(messages.length, 2);
  assert.match(messages[1], /знову доступний/);
});

test("an outage that starts and recovers during quiet hours survives restart as morning digest", async (t) => {
  const { store } = fixture(t);
  let quiet = true; let verdict = "down"; const messages = [];
  const options = { store, probe: async () => result(verdict), isQuiet: () => quiet, notify: async m => messages.push(m) };
  const monitor = createHttpsMonitor(options);
  await monitor.run(); await monitor.run(); verdict = "ok"; await monitor.run();
  assert.equal(messages.length, 0);
  quiet = false;
  const restarted = createHttpsMonitor(options);
  await restarted.run(); await restarted.run();
  assert.equal(messages.length, 1);
  assert.match(messages[0], /2 перевірок із проблемами/);
});

test("additive HTTPS table preserves WS history and persists HTTPS data on reopening", (t) => {
  const { store, db } = fixture(t);
  store.kvSet("telegramUpdateOffset", 123);
  store.recordProbe({ at: new Date(nowMs).toISOString(), verdict: "ok" });
  const value = result(); store.recordHttpsProbe(value);
  const reopened = createHeartbeatStore(db);
  try {
    assert.equal(reopened.getLastProbe().verdict, "ok");
    assert.equal(reopened.kvGet("telegramUpdateOffset"), "123");
    assert.deepEqual(reopened.getLastHttpsProbe(), value);
    assert.equal(reopened.getReadOnlyDb().prepare("SELECT COUNT(*) AS n FROM https_probes").get().n, 1);
  } finally { reopened.close(); }
});

test("HTTPS history endpoint requires the existing API token", async (t) => {
  const { store } = fixture(t);
  store.recordHttpsProbe(result());
  const handler = createApiHandler({ apiToken: "test-password", store, startedAtMs: nowMs, getStatsContext: () => ({}) });
  for (const [headers, expected] of [[{}, 401], [{ authorization: "Bearer test-password" }, 200]]) {
    let status; let payload;
    await handler({ url: "/api/https/probes?limit=1", headers }, {
      writeHead: code => { status = code; }, end: body => { payload = JSON.parse(body); },
    });
    assert.equal(status, expected);
    if (expected === 200) { assert.equal(payload.count, 1); assert.equal(payload.probes[0].verdict, "ok"); }
  }
});

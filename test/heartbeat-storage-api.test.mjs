import assert from "node:assert/strict";
import http from "node:http";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";

import { createApiHandler, sendJson } from "../heartbeat-api.mjs";
import { createHeartbeatStore } from "../heartbeat-storage.mjs";

// Intent: a persisted probe must round-trip every diagnostic scope atomically,
// or API evidence can disagree with the verdict row shown to the operator.
test("SQLite round-trip retains measurement, sync, drain, feed, and raw event context", (t) => {
  const fixture = createStoreFixture(t);
  const probe = sampleProbe();
  fixture.store.recordProbe(probe);
  const restored = fixture.store.withTrades(fixture.store.getProbeById(probe.id));
  assert.equal(restored.referenceTrades, 2);
  assert.deepEqual(restored.trades.map((trade) => [trade.tradeId, trade.delivered]), [
    ["measurement-1", true],
    ["measurement-2", false],
  ]);
  assert.equal(restored.syncTrades[0].tradeId, "sync-1");
  assert.equal(restored.drainTrades[0].tradeId, "drain-1");
  assert.equal(restored.feedTrades[0].matchedScope, "measurement");
  assert.equal(restored.messageEvents[1].rawPreview, "{broken");
  assert.equal(restored.socketEvents[1].eventType, "subscribe_sent");
  assert.deepEqual(restored.socketEvents.at(-1), {
    source: "feed",
    sequence: 3,
    eventType: "close",
    phase: "measuring",
    occurredAtMs: 20_141,
    occurredMonoMs: 141,
    code: 1001,
    reason: "peer restart",
    wasClean: true,
    detail: null,
  });
  assert.equal(restored.phaseCounts.measuring.feed.messages, 2);
  assert.equal(restored.phaseCounts.measuring.feed.trades, 1);
  assert.equal(restored.phaseCounts.measuring.kraken.trades, 2);
  assert.equal(restored.subscribeToFirstTradeMs, 8);
  assert.equal(restored.measurementEndedAtMs - restored.measurementStartedAtMs, 90_000);
  assert.equal(restored.phaseCounts.syncing.kraken.trades, 1);
  assert.equal(restored.phaseCounts.draining.kraken.trades, 1);
  const probeColumns = fixture.store.getReadOnlyDb()
    .prepare("PRAGMA table_info(probes)")
    .all()
    .map((column) => column.name);
  assert.ok(probeColumns.includes("delivery_horizon_feed_messages"));
  assert.ok(probeColumns.includes("measurement_ended_mono_ms"));
  assert.ok(probeColumns.includes("delay_p90_ms"));
  assert.ok(!probeColumns.includes("measurement_feed_messages"));
  assert.ok(!probeColumns.includes("our_trades"));
  assert.deepEqual(restored.lostTrades.map((trade) => trade.tradeId), ["measurement-2"]);
  const later = sampleProbe({ at: "2026-07-20T10:00:00.000Z", verdict: "ok", note: "" });
  fixture.store.recordProbe(later);
  assert.deepEqual(
    fixture.store.getProbesSince(Date.parse("2026-07-20T09:30:00.000Z")).map((entry) => entry.id),
    [later.id],
  );
});

// Intent: a duplicate child row must roll back the parent and every earlier
// insert, preventing a half-probe from becoming the latest server verdict.
test("recordProbe rolls back the entire transaction on a child constraint failure", (t) => {
  const fixture = createStoreFixture(t);
  const probe = sampleProbe();
  probe.feedTrades.push({ ...probe.feedTrades[0] });
  assert.throws(() => fixture.store.recordProbe(probe), /UNIQUE constraint failed/);
  assert.equal(fixture.store.getLastProbe(), null);
  assert.equal(fixture.store.getReadOnlyDb().prepare("SELECT COUNT(*) AS count FROM trades").get().count, 0);
  assert.equal("id" in probe, false);
});

test("schema reset discards probes but preserves Telegram kv state", (t) => {
  const directory = mkdtempSync(path.join(tmpdir(), "heartbeat-reset-"));
  const dbFile = path.join(directory, "heartbeat.db");
  t.after(() => rmSync(directory, { recursive: true, force: true }));

  let store = createHeartbeatStore(dbFile);
  store.kvSet("telegramUpdateOffset", 987);
  store.recordProbe(sampleProbe());
  store.close();

  const db = new DatabaseSync(dbFile);
  db.exec("PRAGMA user_version = 5;");
  db.close();
  store = createHeartbeatStore(dbFile);
  t.after(() => store.close());
  assert.equal(store.kvGet("telegramUpdateOffset"), "987");
  assert.equal(store.getLastProbe(), null);
});

// Intent: authenticated API reads must expose exactly the committed context,
// while unauthenticated and mutating requests must never alter history.
test("HTTP API performs an authenticated storage round-trip and rejects writes", async (t) => {
  const fixture = createStoreFixture(t);
  const first = sampleProbe();
  fixture.store.recordProbe(first);
  const nowMs = Date.parse("2026-07-20T10:00:00Z");
  const handler = createApiHandler({
    apiToken: "test-token",
    store: fixture.store,
    startedAtMs: nowMs - 10_000,
    now: () => nowMs,
    getStatsContext: () => ({
      quietHours: { active: false, label: "23:00-09:00 Europe/Kyiv", timeZone: "Europe/Kyiv" },
      probeRunning: false,
      nextProbeAt: new Date(nowMs + 60_000).toISOString(),
      config: { probeWindowSeconds: 90 },
    }),
  });
  const server = http.createServer((request, response) => {
    handler(request, response).catch((error) => sendJson(response, 500, { error: error.message }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())));
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  const health = await fetch(`${baseUrl}/health`).then(readResponse);
  assert.equal(health.status, 200);
  assert.deepEqual(
    { verdict: health.body.lastVerdict, uptime: health.body.uptimeSeconds },
    { verdict: "degraded", uptime: 10 },
  );

  const unauthorized = await fetch(`${baseUrl}/api/probes/${first.id}`).then(readResponse);
  assert.equal(unauthorized.status, 401);

  const headers = { Authorization: "Bearer test-token" };
  const details = await fetch(`${baseUrl}/api/probes/${first.id}`, { headers }).then(readResponse);
  assert.equal(details.status, 200);
  assert.equal(details.body.trades.length, 2);
  assert.equal(details.body.feedTrades.length, 1);
  assert.equal(details.body.messageEvents.length, 2);
  assert.equal(details.body.socketEvents.length, 4);
  assert.deepEqual(details.body.phaseCounts.measuring, {
    feed: { messages: 2, parsedTrades: 1, parseFailures: 1, trades: 1 },
    kraken: { messages: 0, parsedTrades: 0, parseFailures: 0, trades: 2 },
  });

  const rejectedWrite = await fetch(`${baseUrl}/api/sql`, {
    method: "POST",
    headers,
    body: "DELETE FROM probes",
  }).then(readResponse);
  assert.equal(rejectedWrite.status, 400);
  assert.equal(fixture.store.getLastProbe().id, first.id);

  // Open the read-only connection, then write again through the primary handle:
  // subsequent API queries must observe the new commit under WAL.
  await fetch(`${baseUrl}/api/stats`, { headers }).then(readResponse);
  const second = sampleProbe({ at: "2026-07-20T09:59:00.000Z", verdict: "ok", note: "" });
  fixture.store.recordProbe(second);
  const stats = await fetch(`${baseUrl}/api/stats`, { headers }).then(readResponse);
  assert.equal(stats.body.history.total, 2);
  assert.deepEqual(stats.body.verdictCounts, { degraded: 1, ok: 1 });

  const sql = await fetch(`${baseUrl}/api/sql?q=${encodeURIComponent("SELECT COUNT(*) AS count FROM probes")}`, {
    headers,
  }).then(readResponse);
  assert.equal(sql.status, 200);
  assert.equal(sql.body.rows[0].count, 2);

  const writeThroughCte = await fetch(`${baseUrl}/api/sql`, {
    method: "POST",
    headers,
    body: "WITH doomed AS (SELECT 1) DELETE FROM probes",
  }).then(readResponse);
  assert.equal(writeThroughCte.status, 400);
  assert.equal(fixture.store.getLastProbe().id, second.id);

  const statementBatch = await fetch(`${baseUrl}/api/sql`, {
    method: "POST",
    headers,
    body: "SELECT 1; SELECT 2",
  }).then(readResponse);
  assert.equal(statementBatch.status, 400);

  const hugeSelect = await fetch(`${baseUrl}/api/sql`, {
    method: "POST",
    headers,
    body: "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM n WHERE x < 5001) SELECT x FROM n",
  }).then(readResponse);
  assert.equal(hugeSelect.status, 200);
  assert.equal(hugeSelect.body.rowCount, 5_001);
  assert.equal(hugeSelect.body.truncated, true);
  assert.equal(hugeSelect.body.rows.length, 5_000);

  const injectedVerdict = encodeURIComponent("ok') OR 1=1 --");
  const filtered = await fetch(`${baseUrl}/api/probes?verdict=${injectedVerdict}`, { headers }).then(readResponse);
  assert.equal(filtered.status, 200);
  assert.equal(filtered.body.count, 0);
  const clamped = await fetch(`${baseUrl}/api/probes?limit=-50`, { headers }).then(readResponse);
  assert.equal(clamped.body.count, 1);

  const directHourRows = fixture.store.getReadOnlyDb()
    .prepare("SELECT id, at FROM probes WHERE at >= ? ORDER BY id DESC")
    .all(new Date(nowMs - 3_600_000).toISOString());
  assert.equal(directHourRows.length, 2, JSON.stringify(directHourRows));
  const byHours = await fetch(`${baseUrl}/api/probes?hours=0`, { headers }).then(readResponse);
  assert.equal(byHours.body.count, 2, JSON.stringify(byHours.body));
  const bySince = await fetch(`${baseUrl}/api/probes?since=${encodeURIComponent("2026-07-20T09:58:00Z")}`, { headers }).then(readResponse);
  assert.deepEqual(bySince.body.probes.map((entry) => entry.id), [second.id]);

  const missingProbe = await fetch(`${baseUrl}/api/probes/999999`, { headers }).then(readResponse);
  assert.equal(missingProbe.status, 404);
  const unknownRoute = await fetch(`${baseUrl}/api/unknown`, { headers }).then(readResponse);
  assert.equal(unknownRoute.status, 404);
  const missingSql = await fetch(`${baseUrl}/api/sql`, { headers }).then(readResponse);
  assert.equal(missingSql.status, 400);
});

function createStoreFixture(t) {
  const directory = mkdtempSync(path.join(tmpdir(), "heartbeat-store-"));
  const dbFile = path.join(directory, "heartbeat.db");
  const store = createHeartbeatStore(dbFile);
  t.after(() => {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  });
  return { directory, dbFile, store };
}

function sampleProbe(overrides = {}) {
  const trade = (tradeId, receivedMonoMs, delivered, matchedFeedSequence = null) => ({
    exchangeAtMs: 1_784_538_322_194.771 + receivedMonoMs,
    receivedAtMs: 20_000 + receivedMonoMs,
    receivedMonoMs,
    price: 63_977.5,
    quantity: 0.00078153,
    side: "sell",
    tradeId,
    delivered,
    feedReceivedAtMs: delivered ? 20_031 + receivedMonoMs : null,
    signedDelayMs: delivered ? 31 : null,
    delayMs: delivered ? 31 : null,
    matchedFeedSequence,
  });
  return {
    at: "2026-07-20T09:05:00.000Z",
    startedAtMs: Date.parse("2026-07-20T09:05:00.000Z"),
    startedMonoMs: 0,
    feedSubscribedAtMs: Date.parse("2026-07-20T09:05:00.012Z"),
    feedSubscribedMonoMs: 12,
    firstFeedMessageAtMs: Date.parse("2026-07-20T09:05:00.016Z"),
    firstFeedMessageMonoMs: 16,
    firstFeedTradeAtMs: Date.parse("2026-07-20T09:05:00.020Z"),
    firstFeedTradeMonoMs: 20,
    krakenSubscribedAtMs: Date.parse("2026-07-20T09:05:00.090Z"),
    krakenSubscribedMonoMs: 90,
    measurementStartedAtMs: Date.parse("2026-07-20T09:05:00.100Z"),
    measurementStartedMonoMs: 100,
    measurementEndedAtMs: Date.parse("2026-07-20T09:06:30.100Z"),
    measurementEndedMonoMs: 90_100,
    sessionEndedAtMs: Date.parse("2026-07-20T09:06:40.100Z"),
    sessionEndedMonoMs: 100_100,
    verdict: "degraded",
    note: "missing_trades",
    windowMs: 90_000,
    handshakeMs: 12,
    feedMessages: 3,
    feedParseFailures: 1,
    deliveryHorizonFeedMessages: 2,
    deliveryHorizonFeedParseFailures: 1,
    feedParsedTrades: 1,
    feedPreMeasurementTrades: 0,
    feedSyncTrades: 0,
    feedMeasurementTrades: 1,
    feedDrainTrades: 0,
    krakenMessages: 4,
    krakenParseFailures: 0,
    referenceWindowKrakenParseFailures: 0,
    krakenSyncTrades: 1,
    krakenDrainTrades: 1,
    syncMatched: 0,
    syncCoveragePct: 0,
    feedCandidateTrades: 1,
    referenceTrades: 2,
    matched: 1,
    coveragePct: 50,
    delayMedianMs: 31,
    delayP90Ms: 31,
    delayMaxMs: 31,
    signedDelayMinMs: 31,
    signedDelayMedianMs: 31,
    feedCloses: 0,
    feedErrors: 0,
    krakenCloses: 0,
    krakenErrors: 0,
    trades: [
      trade("measurement-1", 100, true, 1),
      trade("measurement-2", 110, false),
    ],
    syncTrades: [trade("sync-1", 90, false)],
    drainTrades: [trade("drain-1", 200, false)],
    feedTrades: [{
      sequence: 1,
      phase: "measuring",
      exchangeAtMs: 1_784_538_322_194,
      receivedAtMs: 20_131,
      receivedMonoMs: 131,
      price: 63_977.5,
      quantity: 0.00078153,
      side: "sell",
      matchedScope: "measurement",
    }],
    messageEvents: [
      { source: "feed", sequence: 1, phase: "measuring", receivedAtMs: 20_131, receivedMonoMs: 131, parsedTrades: 1, parseFailures: 0 },
      { source: "feed", sequence: 2, phase: "measuring", receivedAtMs: 20_140, receivedMonoMs: 140, parsedTrades: 0, parseFailures: 1, rawPreview: "{broken" },
    ],
    socketEvents: [
      { source: "feed", sequence: 1, eventType: "open", phase: "connecting_feed", occurredAtMs: 20_012, occurredMonoMs: 12 },
      { source: "feed", sequence: 2, eventType: "subscribe_sent", phase: "connecting_feed", occurredAtMs: 20_012, occurredMonoMs: 12 },
      { source: "kraken", sequence: 1, eventType: "subscribe_accepted", phase: "connecting_kraken", occurredAtMs: 20_090, occurredMonoMs: 90 },
      { source: "feed", sequence: 3, eventType: "close", phase: "measuring", occurredAtMs: 20_141, occurredMonoMs: 141, code: 1001, reason: "peer restart", wasClean: true },
    ],
    ...overrides,
  };
}

async function readResponse(response) {
  return { status: response.status, body: await response.json() };
}

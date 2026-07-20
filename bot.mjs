import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

import {
  analyzeLossRuns,
  computeTradeMetrics,
  judgeProbe,
  parseFeedTrade,
  parseKrakenMessage,
  summarizeTradeResults,
} from "./heartbeat-core.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

loadLocalEnv(path.join(__dirname, ".env"));

const config = {
  telegramBotToken: requiredEnv("TELEGRAM_BOT_TOKEN"),
  telegramChatId: requiredEnv("TELEGRAM_CHAT_ID"),
  feedUrl: process.env.FEED_URL || "ws://feed.bitcoinstat.org:9000",
  feedChannel: process.env.FEED_CHANNEL || "trade.btc_usd_kraken",
  krakenUrl: process.env.KRAKEN_URL || "wss://ws.kraken.com/v2",
  krakenSymbol: process.env.KRAKEN_SYMBOL || "BTC/USD",
  probeIntervalMs: Math.max(5, Number(process.env.PROBE_INTERVAL_MINUTES || 60)) * 60_000,
  probeWarmupMs: Math.max(1, Number(process.env.PROBE_WARMUP_SECONDS || 3)) * 1000,
  probePreRollMs: Math.max(1, Number(process.env.PROBE_PRE_ROLL_SECONDS || 2)) * 1000,
  probeWindowMs: Math.max(30, Number(process.env.PROBE_WINDOW_SECONDS || 90)) * 1000,
  probeDrainMs: Math.max(1, Number(process.env.PROBE_DRAIN_SECONDS || 10)) * 1000,
  connectTimeoutMs: Math.max(5, Number(process.env.CONNECT_TIMEOUT_SECONDS || 15)) * 1000,
  confirmDelayMs: Math.max(30, Number(process.env.CONFIRM_DELAY_SECONDS || 120)) * 1000,
  quietHoursTimeZone: process.env.QUIET_HOURS_TIME_ZONE || "Europe/Kyiv",
  quietHoursStartHour: parseHour(process.env.QUIET_HOURS_START || "23:00", "QUIET_HOURS_START"),
  quietHoursEndHour: parseHour(process.env.QUIET_HOURS_END || "09:00", "QUIET_HOURS_END"),
  telegramCommandPollMs: Math.max(3, Number(process.env.TELEGRAM_COMMAND_POLL_SECONDS || 5)) * 1000,
  dbFile: resolveDbFile(),
  runOnce: process.env.RUN_ONCE === "true",
  dryRun: process.env.DRY_RUN === "true",
  // Read-only HTTP API over the collected history. Disabled unless API_TOKEN
  // is set; Railway injects PORT for the public domain.
  apiToken: process.env.API_TOKEN || "",
  // Railway does not always inject PORT; 8080 matches the target port set on
  // the public domain, so the API comes up without any extra variable.
  apiPort: Number(process.env.PORT || process.env.API_PORT || 8080),
};

// Degradation thresholds. Healthy baseline measured 2026-07-19: the feed relays
// Kraken trades with a typical delay of ~7ms and 100% coverage, so these are
// generous — anything beyond them means users actually feel the problem.
const thresholds = {
  slowDelayMs: 5_000, // slowest matched trades later than this => degraded
};

const expectedProbeDurationMs =
  (2 * config.probeWarmupMs) + (2 * config.connectTimeoutMs) + config.probeWindowMs + config.probeDrainMs;

// Storage: SQLite (built into Node, no dependencies). This is a young internal
// diagnostic bot, so probe history is disposable: incompatible schemas reset
// the measurement tables instead of carrying migration code. Service kv state
// survives the reset so Telegram offsets are not replayed.
const SCHEMA_VERSION = 5;
const db = new DatabaseSync(config.dbFile);
db.exec("PRAGMA journal_mode = WAL;");
const schemaVersion = db.prepare("PRAGMA user_version").get().user_version;
if (schemaVersion !== SCHEMA_VERSION) {
  db.exec(`
    DROP TABLE IF EXISTS message_events;
    DROP TABLE IF EXISTS feed_trades;
    DROP TABLE IF EXISTS trades;
    DROP TABLE IF EXISTS probes;
  `);
}
db.exec(`
  CREATE TABLE IF NOT EXISTS probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    measurement_started_at TEXT,
    measurement_started_mono_ms REAL,
    verdict TEXT NOT NULL,
    note TEXT,
    window_seconds INTEGER,
    handshake_ms INTEGER,
    subscribe_to_first_message_ms INTEGER,
    subscribe_to_first_trade_ms INTEGER,
    feed_messages INTEGER,
    feed_parse_failures INTEGER,
    measurement_feed_messages INTEGER,
    measurement_feed_parse_failures INTEGER,
    feed_parsed_trades INTEGER,
    feed_warmup_trades INTEGER,
    feed_sync_trades INTEGER,
    kraken_messages INTEGER,
    kraken_parse_failures INTEGER,
    measurement_kraken_parse_failures INTEGER,
    kraken_sync_trades INTEGER,
    sync_matched INTEGER,
    sync_coverage_pct INTEGER,
    kraken_trades INTEGER,
    our_trades INTEGER,
    reference_trades INTEGER,
    matched INTEGER,
    coverage_pct INTEGER,
    delay_median_ms INTEGER,
    delay_slow_ms INTEGER,
    delay_max_ms INTEGER,
    signed_delay_min_ms INTEGER,
    signed_delay_median_ms INTEGER,
    feed_closes INTEGER,
    feed_errors INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_probes_at ON probes(at);
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id INTEGER NOT NULL REFERENCES probes(id),
    scope TEXT NOT NULL,
    exchange_at_ms REAL NOT NULL,
    kraken_received_at_ms INTEGER NOT NULL,
    kraken_received_mono_ms REAL NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    side TEXT NOT NULL,
    kraken_trade_id TEXT,
    delivered INTEGER NOT NULL DEFAULT 0,
    feed_received_at_ms INTEGER,
    signed_delay_ms INTEGER,
    delay_ms INTEGER,
    matched_feed_sequence INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_trades_probe ON trades(probe_id, scope);
  CREATE TABLE IF NOT EXISTS feed_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id INTEGER NOT NULL REFERENCES probes(id),
    sequence INTEGER NOT NULL,
    phase TEXT NOT NULL,
    exchange_at_ms REAL NOT NULL,
    received_at_ms INTEGER NOT NULL,
    received_mono_ms REAL NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    side TEXT NOT NULL,
    matched_scope TEXT,
    UNIQUE(probe_id, sequence)
  );
  CREATE INDEX IF NOT EXISTS idx_feed_trades_probe ON feed_trades(probe_id, phase);
  CREATE TABLE IF NOT EXISTS message_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id INTEGER NOT NULL REFERENCES probes(id),
    source TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    phase TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    received_mono_ms REAL NOT NULL,
    parsed_trades INTEGER NOT NULL,
    parse_failures INTEGER NOT NULL,
    raw_preview TEXT,
    UNIQUE(probe_id, source, sequence)
  );
  CREATE INDEX IF NOT EXISTS idx_message_events_probe ON message_events(probe_id, source, phase);
  CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
  PRAGMA user_version = ${SCHEMA_VERSION};
`);

// A second, read-only connection to the same file, used only by the HTTP API.
// Declared here rather than in the API section because the server starts
// before the top-level `await heartbeatCycle()` below, and anything declared
// after that await is still in its temporal dead zone while the first probe
// runs — an early request would throw.
const SQL_ROW_LIMIT = 5_000;
let readOnlyDb = null;

const startedAt = new Date();

// Only one probe may run at a time — the feed server degrades under extra
// connections, and overlapping probes would skew each other's numbers.
// Held for the whole cycle including the confirm pause.
let activeProbe = null; // { startedAtMs, windowMs, source }
let lastScheduledCycleAtMs = null;

console.log(
  `Starting trades heartbeat bot. Feed: ${config.feedUrl} (${config.feedChannel}). ` +
    `Reference: ${config.krakenUrl} (${config.krakenSymbol}). ` +
    `Interval: ${Math.round(config.probeIntervalMs / 60_000)} min, ` +
    `warmup: ${Math.round(config.probeWarmupMs / 1000)}s per subscription, ` +
    `pre-roll: ${Math.round(config.probePreRollMs / 1000)}s, ` +
    `window: ${Math.round(config.probeWindowMs / 1000)}s, ` +
    `drain: ${Math.round(config.probeDrainMs / 1000)}s.` +
    `${config.dryRun ? " DRY_RUN: Telegram messages go to console." : ""}`,
);

if (!config.runOnce) {
  startApiServer();
}

// Command polling starts before the first probe and stays responsive while
// subscriptions warm up, the reference window runs, and late trades drain.
if (!config.runOnce) {
  setInterval(() => {
    pollTelegramCommands().catch((error) => {
      console.error("Telegram command polling failed:", error.message);
    });
  }, config.telegramCommandPollMs);

  pollTelegramCommands().catch((error) => {
    console.error("Telegram command polling failed:", error.message);
  });
}

await heartbeatCycle();

if (!config.runOnce) {
  setInterval(() => {
    heartbeatCycle().catch((error) => {
      console.error("Heartbeat cycle failed:", error.message);
    });
  }, config.probeIntervalMs);
}

// --- heartbeat -------------------------------------------------------------

async function heartbeatCycle() {
  if (activeProbe) {
    console.log("Another probe is in progress; skipping this scheduled cycle.");
    return;
  }
  activeProbe = { startedAtMs: Date.now(), windowMs: expectedProbeDurationMs, source: "scheduled" };
  lastScheduledCycleAtMs = Date.now();

  try {
    let probe = await runProbe();
    recordProbe(probe);

    // A broken reference stream cannot be used to judge our feed. Retry once
    // soon instead of waiting for the next hourly cycle.
    if (isKrakenReferenceFailure(probe)) {
      console.log(`Kraken reference failed (${probe.note}). Retrying in ${Math.round(config.confirmDelayMs / 1000)}s.`);
      await sleep(config.confirmDelayMs);
      probe = await runProbe();
      recordProbe(probe);
    }

    // A single bad probe can be a network hiccup on our side. Re-check once
    // after a pause before alarming the chat.
    const lastNotified = kvGet("lastNotifiedVerdict") || "ok";
    if ((probe.verdict === "down" || probe.verdict === "degraded") && probe.verdict !== lastNotified) {
      console.log(`Verdict "${probe.verdict}" differs from last notified "${lastNotified}". Confirming in ${Math.round(config.confirmDelayMs / 1000)}s.`);
      await sleep(config.confirmDelayMs);
      probe = await runProbe();
      recordProbe(probe);
    }

    await maybeNotifyVerdict(probe);
    if (activeProbe.resultChatId) {
      await sendTelegramMessage(activeProbe.resultChatId, formatManualCheckMessage(probe));
    }
  } finally {
    activeProbe = null;
  }
}

// On-demand probe triggered by /check. Replies directly (even during quiet
// hours — it is an explicit request), and refuses to start while any probe
// is already running.
async function runManualCheck(chatId) {
  if (activeProbe) {
    // While a probe runs, repeated /check commands are not processed: the
    // first duplicate gets one short notice, the rest are dropped silently.
    // A scheduled healthy probe is normally silent, so remember this request
    // and explicitly send that probe's final result to the chat.
    if (!activeProbe.notifiedBusy) {
      activeProbe.notifiedBusy = true;
      if (activeProbe.source === "scheduled") activeProbe.resultChatId = chatId;
      const elapsedMs = Date.now() - activeProbe.startedAtMs;
      const remainingSeconds = Math.max(10, Math.round((activeProbe.windowMs - elapsedMs) / 1000));
      await sendTelegramMessage(
        chatId,
        `Перевірка вже триває — результат буде приблизно за ${remainingSeconds} с.`,
      );
    }
    return;
  }
  activeProbe = { startedAtMs: Date.now(), windowMs: expectedProbeDurationMs, source: "manual" };

  try {
    await sendTelegramMessage(
      chatId,
      `Запускаю перевірку: підготую підписки, ${Math.round(config.probeWindowMs / 1000)} с порівнюватиму угоди ` +
        `і ще ${Math.round(config.probeDrainMs / 1000)} с чекатиму на останні доставки. Результат надішлю сюди.`,
    );
    const probe = await runProbe();
    recordProbe(probe);
    await sendTelegramMessage(chatId, formatManualCheckMessage(probe));

    // The chat has just seen the current state, so a scheduled probe with the
    // same verdict should not repeat it.
    if (probe.verdict === "ok" || probe.verdict === "degraded" || probe.verdict === "down") {
      kvSet("lastNotifiedVerdict", probe.verdict);
    }
  } finally {
    activeProbe = null;
  }
}

// Opens one socket to our feed and one to Kraken, listens for a window,
// cross-matches trades by exchange second + price + quantity + side and
// produces a verdict. Receive times remain separate from exchange times.
//
// The feed server never acknowledges subscriptions and silently ignores bad
// channels, so a quiet market and a dead feed look identical from our socket
// alone — Kraken's own stream is the reference that tells them apart.
async function runProbe() {
  const startedAtMs = Date.now();
  const session = await collectSession(config.probeWindowMs, config.probeWarmupMs, config.probeDrainMs);
  const metrics = computeMetrics(session);
  const verdict = judgeProbe(session, metrics, thresholds.slowDelayMs);
  const measurementStartedAt = session.measurementStartedAtMs === null
    ? null
    : new Date(session.measurementStartedAtMs).toISOString();
  const feedWarmupTrades = session.feed.trades.filter(
    (trade) => session.measurementStartedMonoMs !== null && trade.receivedMonoMs < session.measurementStartedMonoMs,
  ).length;
  const feedSyncTrades = session.feed.trades.filter((trade) => trade.phase === "syncing").length;
  const measurementMatchedSequences = new Set(
    metrics.allTrades.map((trade) => trade.matchedFeedSequence).filter((value) => value !== null),
  );
  const syncMatchedSequences = new Set(
    metrics.sync.allTrades.map((trade) => trade.matchedFeedSequence).filter((value) => value !== null),
  );
  const drainMatchedSequences = new Set(
    metrics.drain.allTrades.map((trade) => trade.matchedFeedSequence).filter((value) => value !== null),
  );

  const probe = {
    at: new Date(startedAtMs).toISOString(),
    measurementStartedAt,
    measurementStartedMonoMs: session.measurementStartedMonoMs,
    verdict: verdict.verdict,
    note: verdict.note,
    windowSeconds: Math.round(session.windowMs / 1000),
    handshakeMs: session.feed.handshakeMs,
    subscribeToFirstMessageMs: session.feed.subscribeToFirstMessageMs,
    subscribeToFirstTradeMs: session.feed.subscribeToFirstTradeMs,
    feedMessages: session.feed.messages,
    feedParseFailures: session.feed.parseFailures,
    measurementFeedMessages: metrics.feedMessages,
    measurementFeedParseFailures: metrics.feedParseFailures,
    feedParsedTrades: session.feed.trades.length,
    feedWarmupTrades,
    feedSyncTrades,
    krakenMessages: session.kraken.messages,
    krakenParseFailures: session.kraken.parseFailures,
    measurementKrakenParseFailures: metrics.krakenParseFailures,
    krakenSyncTrades: metrics.sync.referenceTrades,
    syncMatched: metrics.sync.matched,
    syncCoveragePct: metrics.sync.coveragePct,
    krakenTrades: metrics.referenceTrades,
    ourTrades: metrics.feedCandidates.length,
    referenceTrades: metrics.referenceTrades,
    matched: metrics.matched,
    coveragePct: metrics.coveragePct,
    delayMedianMs: metrics.delayMedianMs,
    delaySlowMs: metrics.delaySlowMs,
    delayMaxMs: metrics.delayMaxMs,
    signedDelayMinMs: metrics.signedDelayMinMs,
    signedDelayMedianMs: metrics.signedDelayMedianMs,
    feedCloses: session.feed.closes,
    feedErrors: session.feed.errors,
    // Every measurement and sync reference trade is retained. Every parsed
    // feed trade is retained too, including unmatched warmup/drain records.
    trades: metrics.allTrades,
    syncTrades: metrics.sync.allTrades,
    drainTrades: metrics.drain.allTrades,
    messageEvents: [
      ...session.feed.events.map((event) => ({ ...event, source: "feed" })),
      ...session.kraken.events.map((event) => ({ ...event, source: "kraken" })),
    ],
    feedTrades: session.feed.trades.map((trade) => ({
      ...trade,
      matchedScope: measurementMatchedSequences.has(trade.sequence)
        ? "measurement"
        : syncMatchedSequences.has(trade.sequence)
          ? "sync"
          : drainMatchedSequences.has(trade.sequence)
            ? "drain"
            : null,
    })),
  };
  probe.lostTrades = probe.trades.filter((trade) => !trade.delivered);

  console.log(`Probe: ${JSON.stringify({
    ...probe,
    trades: probe.trades.length,
    syncTrades: probe.syncTrades.length,
    drainTrades: probe.drainTrades.length,
    feedTrades: probe.feedTrades.length,
    messageEvents: probe.messageEvents.length,
    lostTrades: probe.lostTrades.length,
  })}`);
  return probe;
}

function collectSession(windowMs, warmupMs, drainMs) {
  return new Promise((resolve) => {
    const session = {
      windowMs: 0,
      measurementStartedAtMs: null,
      measurementStartedMonoMs: null,
      referenceEndedAtMs: null,
      referenceEndedMonoMs: null,
      endedAtMs: null,
      endedAtMonoMs: null,
      syncStartedAtMs: null,
      syncStartedMonoMs: null,
      feed: {
        handshakeMs: null,
        connectFailed: false,
        trades: [],
        closes: 0,
        errors: 0,
        messages: 0,
        parseFailures: 0,
        events: [],
        subscribedAtMs: null,
        subscribedAtMonoMs: null,
        subscribeToFirstMessageMs: null,
        subscribeToFirstTradeMs: null,
      },
      kraken: {
        connected: false,
        connectFailed: false,
        disconnected: false,
        trades: [],
        messages: 0,
        parseFailures: 0,
        events: [],
      },
    };

    const connectionStartedAtMs = Date.now();
    let finished = false;
    let phase = "connecting_feed";
    let feedSocket = null;
    let krakenSocket = null;
    let feedConnectTimer = null;
    let warmupTimer = null;
    let krakenConnectTimer = null;
    let syncTimer = null;
    let measurementTimer = null;
    let drainTimer = null;

    const finish = () => {
      if (finished) return;
      finished = true;
      phase = "finished";
      for (const timer of [feedConnectTimer, warmupTimer, krakenConnectTimer, syncTimer, measurementTimer, drainTimer]) {
        if (timer) clearTimeout(timer);
      }
      session.endedAtMs = Date.now();
      session.endedAtMonoMs = performance.now();
      if (session.measurementStartedMonoMs !== null) {
        session.windowMs =
          (session.referenceEndedMonoMs ?? session.endedAtMonoMs) - session.measurementStartedMonoMs;
      }
      for (const socket of [feedSocket, krakenSocket]) {
        try { socket?.close(); } catch { /* already closed */ }
      }
      resolve(session);
    };

    const beginMeasurement = () => {
      if (finished || phase !== "syncing") return;

      // Keep the overlapping sync history. Exact monotonic boundaries select
      // the measurement reference later; the feed pre-roll remains available
      // for copies that reached this process before Kraken did.
      session.measurementStartedAtMs = Date.now();
      session.measurementStartedMonoMs = performance.now();
      phase = "measuring";

      measurementTimer = setTimeout(() => {
        if (finished || phase !== "measuring") return;
        session.referenceEndedAtMs = Date.now();
        session.referenceEndedMonoMs = performance.now();
        phase = "draining";

        // The reference window is fixed, but keep Kraken open during drain as
        // non-verdict context. Post-window Kraken trades reserve their own feed
        // copies so an equal-value new trade cannot backfill an in-window loss.
        drainTimer = setTimeout(finish, drainMs);
      }, windowMs);
    };

    const connectKraken = () => {
      if (finished) return;
      phase = "connecting_kraken";

      try {
        krakenSocket = new WebSocket(config.krakenUrl);
      } catch {
        session.kraken.connectFailed = true;
        finish();
        return;
      }

      // This timeout includes both the WebSocket handshake and Kraken's
      // subscription acknowledgement.
      krakenConnectTimer = setTimeout(() => {
        if (phase === "connecting_kraken") {
          session.kraken.connectFailed = true;
          finish();
        }
      }, config.connectTimeoutMs);

      krakenSocket.onopen = () => {
        session.kraken.connected = true;
        krakenSocket.send(
          JSON.stringify({ method: "subscribe", params: { channel: "trade", symbol: [config.krakenSymbol], snapshot: false } }),
        );
      };
      krakenSocket.onmessage = (event) => {
        if (finished) return;
        const receivedAtMs = Date.now();
        const receivedMonoMs = performance.now();
        session.kraken.messages += 1;
        const message = parseKrakenMessage(event.data, receivedAtMs, receivedMonoMs);
        session.kraken.parseFailures += message.parseFailures;
        session.kraken.events.push({
          sequence: session.kraken.messages,
          phase,
          receivedAtMs,
          receivedMonoMs,
          parsedTrades: message.trades.length,
          parseFailures: message.parseFailures,
          rawPreview: message.parseFailures > 0 ? previewRawMessage(event.data) : null,
        });
        if (message.subscription === "accepted" && phase === "connecting_kraken") {
          clearTimeout(krakenConnectTimer);
          session.syncStartedAtMs = receivedAtMs;
          session.syncStartedMonoMs = receivedMonoMs;
          phase = "syncing";
          syncTimer = setTimeout(beginMeasurement, warmupMs);
          return;
        }
        if (message.subscription === "rejected") {
          session.kraken.connectFailed = true;
          console.error(`Kraken subscription failed: ${message.error || "unknown error"}`);
          finish();
          return;
        }
        if (phase === "syncing" || phase === "measuring" || phase === "draining") {
          session.kraken.trades.push(...message.trades.map((trade) => ({ ...trade, phase })));
        }
      };
      krakenSocket.onerror = () => {
        if (phase === "connecting_kraken") {
          session.kraken.connectFailed = true;
          finish();
        } else if (phase === "syncing" || phase === "measuring") {
          session.kraken.disconnected = true;
          finish();
        }
      };
      krakenSocket.onclose = () => {
        if (finished || phase === "draining") return;
        if (phase === "connecting_kraken") {
          session.kraken.connectFailed = true;
        } else if (phase === "syncing" || phase === "measuring") {
          session.kraken.disconnected = true;
        }
        finish();
      };
    };

    try {
      feedSocket = new WebSocket(config.feedUrl);
    } catch {
      session.feed.connectFailed = true;
      finish();
      return;
    }

    feedConnectTimer = setTimeout(() => {
      if (session.feed.handshakeMs === null) {
        session.feed.connectFailed = true;
        finish();
      }
    }, config.connectTimeoutMs);

    feedSocket.onopen = () => {
      clearTimeout(feedConnectTimer);
      session.feed.handshakeMs = Date.now() - connectionStartedAtMs;
      session.feed.subscribedAtMs = Date.now();
      session.feed.subscribedAtMonoMs = performance.now();
      feedSocket.send(JSON.stringify({ subscribe: config.feedChannel }));
      phase = "warming_feed";
      warmupTimer = setTimeout(connectKraken, warmupMs);
    };
    feedSocket.onmessage = (event) => {
      if (finished) return;
      const receivedAtMs = Date.now();
      const receivedMonoMs = performance.now();
      session.feed.messages += 1;
      if (session.feed.subscribeToFirstMessageMs === null && session.feed.subscribedAtMonoMs !== null) {
        session.feed.subscribeToFirstMessageMs = Math.round(receivedMonoMs - session.feed.subscribedAtMonoMs);
      }
      const parsed = parseFeedTrade(event.data, receivedAtMs, receivedMonoMs);
      session.feed.parseFailures += parsed.parseFailures;
      session.feed.events.push({
        sequence: session.feed.messages,
        phase,
        receivedAtMs,
        receivedMonoMs,
        parsedTrades: parsed.trade ? 1 : 0,
        parseFailures: parsed.parseFailures,
        rawPreview: parsed.parseFailures > 0 ? previewRawMessage(event.data) : null,
      });
      if (parsed.trade) {
        if (session.feed.subscribeToFirstTradeMs === null && session.feed.subscribedAtMonoMs !== null) {
          session.feed.subscribeToFirstTradeMs = Math.round(receivedMonoMs - session.feed.subscribedAtMonoMs);
        }
        session.feed.trades.push({
          ...parsed.trade,
          phase,
          sequence: session.feed.messages,
        });
      }
    };
    feedSocket.onerror = () => {
      session.feed.errors += 1;
      if (phase === "connecting_feed" || phase === "warming_feed" || phase === "connecting_kraken" || phase === "syncing") {
        session.feed.connectFailed = true;
        finish();
      }
    };
    feedSocket.onclose = () => {
      if (finished) return;
      session.feed.closes += 1;
      if (phase === "connecting_feed" || phase === "warming_feed" || phase === "connecting_kraken" || phase === "syncing") {
        session.feed.connectFailed = true;
        finish();
      }
    };
  });
}

function computeMetrics(session) {
  if (session.measurementStartedMonoMs === null) {
    return {
      ...computeTradeMetrics([], []),
      feedCandidates: [],
      sync: computeTradeMetrics([], []),
      drain: computeTradeMetrics([], []),
      feedMessages: 0,
      feedParseFailures: 0,
      krakenParseFailures: 0,
    };
  }

  const referenceEnd = session.referenceEndedMonoMs ?? session.endedAtMonoMs ?? performance.now();
  const contextStart = session.syncStartedMonoMs ?? session.measurementStartedMonoMs;
  const contextEnd = session.endedAtMonoMs ?? performance.now();
  const contextReference = session.kraken.trades.filter(
    (trade) => trade.receivedMonoMs >= contextStart && trade.receivedMonoMs <= contextEnd,
  );
  const contextFeedCandidates = session.feed.trades.filter(
    (trade) =>
      trade.receivedMonoMs >= contextStart - config.probePreRollMs &&
      trade.receivedMonoMs <= contextEnd,
  );
  const context = computeTradeMetrics(contextReference, contextFeedCandidates, {
    maxLeadMs: config.probePreRollMs,
    maxLagMs: config.probeDrainMs,
  });
  const measurementTrades = context.allTrades.filter(
    (trade) =>
      trade.receivedMonoMs >= session.measurementStartedMonoMs && trade.receivedMonoMs < referenceEnd,
  );
  const feedCandidates = session.feed.trades.filter(
    (trade) =>
      trade.receivedMonoMs >= session.measurementStartedMonoMs - config.probePreRollMs &&
      trade.receivedMonoMs <= referenceEnd + config.probeDrainMs,
  );
  const syncTrades = context.allTrades.filter(
    (trade) => trade.receivedMonoMs < session.measurementStartedMonoMs,
  );
  const drainTrades = context.allTrades.filter((trade) => trade.receivedMonoMs >= referenceEnd);
  const measurementFeedEvents = session.feed.events.filter(
    (event) =>
      event.receivedMonoMs >= session.measurementStartedMonoMs && event.receivedMonoMs <= contextEnd,
  );
  const measurementKrakenEvents = session.kraken.events.filter(
    (event) =>
      event.receivedMonoMs >= session.measurementStartedMonoMs && event.receivedMonoMs < referenceEnd,
  );
  return {
    ...summarizeTradeResults(measurementTrades),
    feedCandidates,
    sync: summarizeTradeResults(syncTrades),
    drain: summarizeTradeResults(drainTrades),
    feedMessages: measurementFeedEvents.length,
    feedParseFailures: measurementFeedEvents.reduce((sum, event) => sum + event.parseFailures, 0),
    krakenParseFailures: measurementKrakenEvents.reduce((sum, event) => sum + event.parseFailures, 0),
  };
}

function previewRawMessage(raw) {
  const text = typeof raw === "string"
    ? raw
    : raw instanceof ArrayBuffer
      ? Buffer.from(raw).toString("utf8")
      : ArrayBuffer.isView(raw)
        ? Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength).toString("utf8")
        : String(raw);
  return text.slice(0, 500);
}

function recordProbe(probe) {
  db.exec("BEGIN");
  try {
    const info = db.prepare(`
      INSERT INTO probes (
        at, measurement_started_at, measurement_started_mono_ms, verdict, note, window_seconds, handshake_ms,
        subscribe_to_first_message_ms, subscribe_to_first_trade_ms,
        feed_messages, feed_parse_failures, measurement_feed_messages, measurement_feed_parse_failures,
        feed_parsed_trades, feed_warmup_trades, feed_sync_trades,
        kraken_messages, kraken_parse_failures, measurement_kraken_parse_failures,
        kraken_sync_trades, sync_matched, sync_coverage_pct,
        kraken_trades, our_trades, reference_trades, matched, coverage_pct,
        delay_median_ms, delay_slow_ms, delay_max_ms, signed_delay_min_ms, signed_delay_median_ms,
        feed_closes, feed_errors
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      probe.at, probe.measurementStartedAt ?? null, probe.measurementStartedMonoMs ?? null,
      probe.verdict, probe.note ?? null,
      probe.windowSeconds ?? null, probe.handshakeMs ?? null,
      probe.subscribeToFirstMessageMs ?? null, probe.subscribeToFirstTradeMs ?? null,
      probe.feedMessages ?? null, probe.feedParseFailures ?? null,
      probe.measurementFeedMessages ?? null, probe.measurementFeedParseFailures ?? null,
      probe.feedParsedTrades ?? null, probe.feedWarmupTrades ?? null, probe.feedSyncTrades ?? null,
      probe.krakenMessages ?? null, probe.krakenParseFailures ?? null,
      probe.measurementKrakenParseFailures ?? null, probe.krakenSyncTrades ?? null,
      probe.syncMatched ?? null, probe.syncCoveragePct ?? null,
      probe.krakenTrades ?? null, probe.ourTrades ?? null, probe.referenceTrades ?? null,
      probe.matched ?? null, probe.coveragePct ?? null, probe.delayMedianMs ?? null,
      probe.delaySlowMs ?? null, probe.delayMaxMs ?? null, probe.signedDelayMinMs ?? null,
      probe.signedDelayMedianMs ?? null, probe.feedCloses ?? null, probe.feedErrors ?? null,
    );
    probe.id = Number(info.lastInsertRowid);
    insertReferenceTrades(probe.id, "measurement", probe.trades || []);
    insertReferenceTrades(probe.id, "sync", probe.syncTrades || []);
    insertReferenceTrades(probe.id, "drain", probe.drainTrades || []);
    insertFeedTrades(probe.id, probe.feedTrades || []);
    insertMessageEvents(probe.id, probe.messageEvents || []);
    db.exec("COMMIT");
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

function insertReferenceTrades(probeId, scope, trades) {
  const statement = db.prepare(
    `INSERT INTO trades (
      probe_id, scope, exchange_at_ms, kraken_received_at_ms, kraken_received_mono_ms, price, quantity, side,
      kraken_trade_id, delivered, feed_received_at_ms, signed_delay_ms, delay_ms, matched_feed_sequence
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  for (const trade of trades) {
    statement.run(
      probeId, scope, trade.exchangeAtMs, trade.receivedAtMs, trade.receivedMonoMs, trade.price, trade.quantity,
      trade.side, trade.tradeId ?? null, trade.delivered ? 1 : 0, trade.feedReceivedAtMs ?? null,
      trade.signedDelayMs ?? null, trade.delayMs ?? null, trade.matchedFeedSequence ?? null,
    );
  }
}

function insertFeedTrades(probeId, trades) {
  const statement = db.prepare(
    `INSERT INTO feed_trades (
      probe_id, sequence, phase, exchange_at_ms, received_at_ms, received_mono_ms,
      price, quantity, side, matched_scope
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  for (const trade of trades) {
    statement.run(
      probeId, trade.sequence, trade.phase, trade.exchangeAtMs, trade.receivedAtMs, trade.receivedMonoMs,
      trade.price, trade.quantity, trade.side, trade.matchedScope ?? null,
    );
  }
}

function insertMessageEvents(probeId, events) {
  const statement = db.prepare(
    `INSERT INTO message_events (
      probe_id, source, sequence, phase, received_at_ms, received_mono_ms,
      parsed_trades, parse_failures, raw_preview
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  );
  for (const event of events) {
    statement.run(
      probeId, event.source, event.sequence, event.phase, event.receivedAtMs, event.receivedMonoMs,
      event.parsedTrades, event.parseFailures, event.rawPreview ?? null,
    );
  }
}

function rowToProbe(row) {
  if (!row) return null;
  return {
    id: row.id,
    at: row.at,
    measurementStartedAt: row.measurement_started_at,
    measurementStartedMonoMs: row.measurement_started_mono_ms,
    verdict: row.verdict,
    note: row.note ?? "",
    windowSeconds: row.window_seconds,
    handshakeMs: row.handshake_ms,
    subscribeToFirstMessageMs: row.subscribe_to_first_message_ms,
    subscribeToFirstTradeMs: row.subscribe_to_first_trade_ms,
    feedMessages: row.feed_messages,
    feedParseFailures: row.feed_parse_failures,
    measurementFeedMessages: row.measurement_feed_messages,
    measurementFeedParseFailures: row.measurement_feed_parse_failures,
    feedParsedTrades: row.feed_parsed_trades,
    feedWarmupTrades: row.feed_warmup_trades,
    feedSyncTrades: row.feed_sync_trades,
    krakenMessages: row.kraken_messages,
    krakenParseFailures: row.kraken_parse_failures,
    measurementKrakenParseFailures: row.measurement_kraken_parse_failures,
    krakenSyncTrades: row.kraken_sync_trades,
    syncMatched: row.sync_matched,
    syncCoveragePct: row.sync_coverage_pct,
    krakenTrades: row.kraken_trades,
    ourTrades: row.our_trades,
    referenceTrades: row.reference_trades,
    matched: row.matched,
    coveragePct: row.coverage_pct,
    delayMedianMs: row.delay_median_ms,
    delaySlowMs: row.delay_slow_ms,
    delayMaxMs: row.delay_max_ms,
    signedDelayMinMs: row.signed_delay_min_ms,
    signedDelayMedianMs: row.signed_delay_median_ms,
    feedCloses: row.feed_closes,
    feedErrors: row.feed_errors,
  };
}

function getLastProbe() {
  return rowToProbe(db.prepare("SELECT * FROM probes ORDER BY id DESC LIMIT 1").get());
}

function getProbeById(id) {
  return rowToProbe(db.prepare("SELECT * FROM probes WHERE id = ?").get(id));
}

function getProbesSince(sinceMs) {
  return db
    .prepare("SELECT * FROM probes WHERE at >= ? ORDER BY id")
    .all(new Date(sinceMs).toISOString())
    .map(rowToProbe);
}

function getTrades(probeId, scope = "measurement") {
  return db
    .prepare(`SELECT
      exchange_at_ms, kraken_received_at_ms, kraken_received_mono_ms, price, quantity, side, kraken_trade_id,
      delivered, feed_received_at_ms, signed_delay_ms, delay_ms, matched_feed_sequence
      FROM trades WHERE probe_id = ? AND scope = ? ORDER BY exchange_at_ms, kraken_received_at_ms, id`)
    .all(probeId, scope)
    .map((row) => ({
      exchangeAtMs: row.exchange_at_ms,
      receivedAtMs: row.kraken_received_at_ms,
      receivedMonoMs: row.kraken_received_mono_ms,
      price: row.price,
      quantity: row.quantity,
      side: row.side,
      tradeId: row.kraken_trade_id,
      delivered: row.delivered === 1,
      feedReceivedAtMs: row.feed_received_at_ms,
      signedDelayMs: row.signed_delay_ms,
      delayMs: row.delay_ms,
      matchedFeedSequence: row.matched_feed_sequence,
    }));
}

function getFeedTrades(probeId) {
  return db
    .prepare(`SELECT sequence, phase, exchange_at_ms, received_at_ms, received_mono_ms,
        price, quantity, side, matched_scope
      FROM feed_trades WHERE probe_id = ? ORDER BY sequence`)
    .all(probeId)
    .map((row) => ({
      sequence: row.sequence,
      phase: row.phase,
      exchangeAtMs: row.exchange_at_ms,
      receivedAtMs: row.received_at_ms,
      receivedMonoMs: row.received_mono_ms,
      price: row.price,
      quantity: row.quantity,
      side: row.side,
      matchedScope: row.matched_scope,
    }));
}

function getMessageEvents(probeId) {
  return db
    .prepare(`SELECT source, sequence, phase, received_at_ms, received_mono_ms,
        parsed_trades, parse_failures, raw_preview
      FROM message_events WHERE probe_id = ? ORDER BY received_at_ms, id`)
    .all(probeId)
    .map((row) => ({
      source: row.source,
      sequence: row.sequence,
      phase: row.phase,
      receivedAtMs: row.received_at_ms,
      receivedMonoMs: row.received_mono_ms,
      parsedTrades: row.parsed_trades,
      parseFailures: row.parse_failures,
      rawPreview: row.raw_preview,
    }));
}

function withTrades(probe) {
  if (!probe) return null;
  const trades = getTrades(probe.id);
  return {
    ...probe,
    trades,
    syncTrades: getTrades(probe.id, "sync"),
    drainTrades: getTrades(probe.id, "drain"),
    feedTrades: getFeedTrades(probe.id),
    messageEvents: getMessageEvents(probe.id),
    lostTrades: trades.filter((trade) => !trade.delivered),
  };
}

function kvGet(key) {
  return db.prepare("SELECT value FROM kv WHERE key = ?").get(key)?.value;
}

function kvSet(key, value) {
  db.prepare(
    "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
  ).run(key, String(value));
}

// --- notifications ---------------------------------------------------------

// Sends every degraded/down result and sends ok only when recovering from the
// last *notified* problem verdict.
// During quiet hours nothing is sent and lastNotifiedVerdict stays unchanged,
// so the first probe after quiet hours delivers the catch-up automatically.
// Kraken reference failures notify separately after their automatic retry but
// never reset the last known verdict of our feed.
async function maybeNotifyVerdict(probe) {
  if (isKrakenReferenceFailure(probe)) {
    if (isQuietHours()) {
      console.log(`Quiet hours: holding back "${probe.note}" notification.`);
      return;
    }
    await notify(formatKrakenReferenceFailureMessage(probe));
    return;
  }

  if (probe.verdict !== "ok" && probe.verdict !== "degraded" && probe.verdict !== "down") return;

  const lastNotified = kvGet("lastNotifiedVerdict") || "ok";
  const isProblem = probe.verdict === "degraded" || probe.verdict === "down";
  if (!isProblem && probe.verdict === lastNotified) return;

  if (isQuietHours()) {
    console.log(`Quiet hours: holding back "${probe.verdict}" notification (last notified: "${lastNotified}").`);
    return;
  }

  await notify(formatVerdictChangeMessage(probe, lastNotified));
  kvSet("lastNotifiedVerdict", probe.verdict);
}

async function notify(text, { ignoreQuietHours = false } = {}) {
  if (!ignoreQuietHours && isQuietHours()) return;
  await sendTelegramMessage(config.telegramChatId, text);
}

// --- message formatting ----------------------------------------------------

function formatVerdictChangeMessage(probe, previousVerdict) {
  const time = formatDateTime(probe.at);

  if (probe.verdict === "down") {
    return [
      "🔴 <b>Стрічка угод не працює</b>",
      "",
      `Перевірка о ${time}. ${describeProblemDetail(probe)}`,
      "Користувачі зараз не бачать нових угод у застосунку.",
    ].join("\n");
  }

  if (probe.verdict === "degraded") {
    return [
      "🟠 <b>Стрічка угод працює з перебоями</b>",
      "",
      `Перевірка о ${time}. ${describeProblemDetail(probe)}`,
    ].join("\n");
  }

  const recoveryDetail = probe.matched > 0 && probe.delayMedianMs !== null
    ? `Дійшли всі ${probe.matched} ${tradesWord(probe.matched)}, угоди долітали за ${formatDelay(probe.delayMedianMs)}.`
    : "Угоди знову доходять без затримок.";
  return [
    "🟢 <b>Стрічка угод знову працює нормально</b>",
    "",
    `Перевірка о ${time}. ${recoveryDetail}`,
    previousVerdict === "down" ? "До цього сервер не працював." : "До цього сервер працював з перебоями.",
  ].join("\n");
}

function formatKrakenReferenceFailureMessage(probe) {
  return [
    "⚪ <b>Не вдалося перевірити стрічку угод</b>",
    "",
    describeInconclusiveDetail(probe),
    "Автоматична повторна перевірка також не дала повного потоку Kraken. Стан BitcoinTicker зараз невідомий.",
  ].join("\n");
}

function formatManualCheckMessage(probe) {
  const lines = [`Перевірка завершена (слухав ${probe.windowSeconds} с).`, ""];
  lines.push(`Стан: ${describeVerdictLine(probe)}`);

  if (probe.verdict === "inconclusive") {
    lines.push(describeInconclusiveDetail(probe));
    return lines.join("\n");
  }

  if (probe.verdict === "ok") {
    const total = referenceCount(probe);
    lines.push(
      `Біржа показала ${total} ${tradesWord(total)}, наш сервер передав ${probe.coveragePct}% з них. ` +
        `Зазвичай угода долітала за ${formatDelay(probe.delayMedianMs)}, найповільніша — за ${formatDelay(probe.delayMaxMs)}.`,
    );
  } else {
    const detail = describeProblemDetail(probe);
    if (detail) lines.push(detail);
  }

  return lines.join("\n");
}

function isKrakenReferenceFailure(probe) {
  return probe.note === "kraken_unavailable" ||
    probe.note === "kraken_disconnected" ||
    probe.note === "kraken_parse_failure";
}

function describeInconclusiveDetail(probe) {
  if (probe.note === "kraken_disconnected") {
    return "Kraken розірвав з’єднання під час перевірки. Еталонний потік неповний, тому стан BitcoinTicker оцінити неможливо.";
  }
  if (probe.note === "kraken_unavailable") {
    return "Біржа Kraken була недоступна, тому стан BitcoinTicker оцінити неможливо.";
  }
  if (probe.note === "kraken_parse_failure") {
    return "Kraken надіслав пошкоджене або неочікуване повідомлення під час вікна. Еталон неповний, тому стан BitcoinTicker оцінити неможливо.";
  }
  return "За час перевірки Kraken не передав жодної угоди, тому порівнювати нема чого. Спробуйте /check трохи пізніше.";
}

// One sentence with the concrete numbers behind a bad verdict, shared by
// alerts, /check and /stats so every surface explains what exactly is wrong.
function describeProblemDetail(probe) {
  const total = probe.matched + missingCount(probe);
  switch (probe.note) {
    case "connect_failed":
      return "Сервер не відповідає: не вдалося встановити з'єднання.";
    case "feed_silent":
    case "no_matches":
      return probe.ourTrades > 0
        ? `Біржа показала ${probe.krakenTrades} ${tradesWord(probe.krakenTrades)}, сервер надіслав ${probe.ourTrades} повідомлень, але реальних угод з біржі серед них немає — дані не збігаються.`
        : `Біржа показала ${probe.krakenTrades} ${tradesWord(probe.krakenTrades)} за ${probe.windowSeconds} с, наш сервер не передав жодної.`;
    case "invalid_feed_messages":
      return `Фід надіслав ${probe.measurementFeedMessages} повідомлень, але ${probe.measurementFeedParseFailures} з них не вдалося розібрати. Дані фіда пошкоджені або змінили формат.`;
    case "socket_dropped":
      return `Сервер обривав з'єднання під час перевірки. Дійшло ${probe.matched} з ${total} угод (${probe.coveragePct}%).`;
    case "missing_trades":
      return `Частина угод губиться: з ${total} угод на біржі дійшло лише ${probe.matched} (${probe.coveragePct}%). Ті, що дійшли, долітали зазвичай за ${formatDelay(probe.delayMedianMs)}.`;
    case "slow_delivery":
      return `Угоди доходять (${probe.coveragePct}%), але повільно: зазвичай за ${formatDelay(probe.delayMedianMs)}, найповільніші — за ${formatDelay(probe.delaySlowMs)}. У нормі — менш ніж пів секунди.`;
    default:
      return null;
  }
}

// Full breakdown of one probe, including every lost trade with its
// exchange-side timestamp — evidence for debugging the feed server itself.
function formatDetailsMessages(probe) {
  const lines = [
    `${verdictEmoji(probe.verdict)} <b>Перевірка №${probe.id ?? "—"}</b> — ${detailsHeadline(probe)}`,
    `<i>${formatDateTime(probe.at)} · вікно ${probe.windowSeconds} с</i>`,
  ];
  if (probe.verdict === "inconclusive") {
    lines.push("", describeInconclusiveDetail(probe));
    return [lines.join("\n")];
  }

  lines.push(
    "",
    `Угод на біржі: ${referenceCount(probe)}`,
    `Дійшло: ${probe.matched}${probe.coveragePct !== null ? ` (${probe.coveragePct}%)` : ""}`,
  );
  if (probe.delayMedianMs !== null && probe.delayMedianMs !== undefined) {
    lines.push(`Затримка: зазвичай ${formatDelay(probe.delayMedianMs)}, максимум ${formatDelay(probe.delayMaxMs)}`);
  }
  if (probe.handshakeMs !== null && probe.handshakeMs !== undefined) {
    lines.push(`Підключення: ${formatDelay(probe.handshakeMs)}`);
  }
  if (probe.subscribeToFirstTradeMs !== null && probe.subscribeToFirstTradeMs !== undefined) {
    lines.push(`Перша угода після підписки: ${formatDelay(probe.subscribeToFirstTradeMs)}`);
  }
  if (probe.krakenSyncTrades > 0) {
    lines.push(
      `Перекривний прогрів: ${probe.syncMatched} з ${probe.krakenSyncTrades}` +
        `${probe.syncCoveragePct !== null ? ` (${probe.syncCoveragePct}%)` : ""}`,
    );
  }
  if (probe.measurementFeedParseFailures || probe.measurementKrakenParseFailures) {
    lines.push(
      `Помилки розбору у вікні: feed ${probe.measurementFeedParseFailures || 0}, ` +
        `Kraken ${probe.measurementKrakenParseFailures || 0}`,
    );
  }
  if (probe.feedCloses || probe.feedErrors) {
    lines.push(`Обриви з'єднання: ${probe.feedCloses}, помилки сокета: ${probe.feedErrors}`);
  }

  const lost = probe.lostTrades || [];
  if (lost.length === 0) {
    lines.push(
      "",
      (probe.coveragePct ?? 100) < 100
        ? "Перелік загублених угод для цієї перевірки не зберігся."
        : "Загублених угод не було.",
    );
    return [lines.join("\n")];
  }

  // Full trade log: every reference trade in order with its status. The ✓/✗
  // column shows the dynamics top-down, the rest of the row carries the
  // trade itself. Telegram caps a message at 4096 characters, so the log is
  // split into as many <pre> blocks (one per message) as needed — never
  // truncated.
  const trades = probe.trades || [];
  const pattern = describeLossPattern(trades);

  lines.push("", "<b>Угоди</b> (✓ дійшла · ✗ загублена)");
  const priceWidth = Math.max(...trades.map((trade) => String(trade.price).length));
  const rows = trades.map((trade) =>
    `${trade.delivered ? "✓" : "✗"} ${formatTimeWithSeconds(trade.exchangeAtMs)}  ${String(trade.price).padStart(priceWidth)}  ${trade.quantity} ${trade.side}`,
  );
  const blocks = chunkRowsByLength(rows, 3500).map((chunk) => `<pre>${chunk.join("\n")}</pre>`);

  const header = lines.join("\n");
  const singleMessage = `${header}\n${blocks[0]}\n\n${pattern}`;
  if (blocks.length === 1 && singleMessage.length <= TELEGRAM_MESSAGE_LIMIT) {
    return [singleMessage];
  }

  const messages = [header, ...blocks];
  const lastWithPattern = `${messages[messages.length - 1]}\n\n${pattern}`;
  if (lastWithPattern.length <= TELEGRAM_MESSAGE_LIMIT) {
    messages[messages.length - 1] = lastWithPattern;
  } else {
    messages.push(pattern);
  }
  return messages;
}

const TELEGRAM_MESSAGE_LIMIT = 4096;

// Groups lines so each group's total length stays under maxLength.
function chunkRowsByLength(rows, maxLength) {
  const chunks = [];
  let current = [];
  let length = 0;
  for (const row of rows) {
    if (current.length > 0 && length + row.length + 1 > maxLength) {
      chunks.push(current);
      current = [];
      length = 0;
    }
    current.push(row);
    length += row.length + 1;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

// Compact headline for /details, where the numbers block below already
// carries the figures — no percentages repeated here.
function detailsHeadline(probe) {
  if (probe.verdict === "ok") return "сервер працював нормально";
  if (probe.note === "kraken_disconnected") return "Kraken розірвав з’єднання";
  if (probe.note === "kraken_unavailable") return "Kraken був недоступний";
  if (probe.note === "kraken_parse_failure") return "повідомлення Kraken не вдалося розібрати";
  if (probe.verdict === "inconclusive") return "ринок був надто тихий, щоб оцінити";
  const label = {
    connect_failed: "сервер не відповідав",
    feed_silent: "угоди не доходили",
    no_matches: "дані не збігалися з біржею",
    invalid_feed_messages: "фід надсилав пошкоджені дані",
    missing_trades: "частина угод губилася",
    slow_delivery: "угоди доходили із запізненням",
    socket_dropped: "сервер обривав з'єднання",
  }[probe.note];
  return label || (probe.verdict === "down" ? "сервер не працював" : "сервер працював з перебоями");
}

// Describe observed consecutive loss runs without guessing their server-side
// cause. Delivered trades split runs; quiet exchange gaps over two seconds do
// too. Exchange timestamps prevent Kraken packet batching from inventing runs.
function describeLossPattern(trades) {
  const analysis = analyzeLossRuns(trades);
  if (analysis.runs.length === 0) return "Загублених угод не було.";

  const positionLabel = {
    all: "протягом усього вікна",
    start: "на початку вікна",
    middle: "посередині вікна",
    end: "наприкінці вікна",
  };
  const descriptions = analysis.runs.map((run) => {
    const duration = run.durationMs > 0 ? ` за ${formatDelay(run.durationMs)}` : "";
    return `${run.count} ${tradesWord(run.count)} ${positionLabel[run.position]}${duration}`;
  });
  return analysis.runs.length === 1
    ? `Втрати утворили одну безперервну серію: ${descriptions[0]}.`
    : `Серії втрат (${analysis.runs.length}): ${descriptions.join("; ")}.`;
}

function formatStatsMessage() {
  const now = new Date();
  const probe = getLastProbe();
  const quietNow = isQuietHours(now);

  const lines = ["<b>Trades heartbeat</b>", ""];

  if (!probe) {
    lines.push("Перевірок ще не було.");
  } else {
    lines.push(`Стан: ${describeVerdictLine(probe)}`);
    lines.push(`Остання перевірка: ${formatDateTime(probe.at)} (вікно ${probe.windowSeconds} с).`);
    if (probe.verdict === "ok" && probe.delayMedianMs !== null) {
      const total = referenceCount(probe);
      lines.push(
        `Біржа показала ${total} ${tradesWord(total)}, наш сервер передав ${probe.coveragePct}% з них. ` +
          `Зазвичай угода долітає за ${formatDelay(probe.delayMedianMs)}.`,
      );
    } else if (probe.verdict === "degraded" || probe.verdict === "down") {
      const detail = describeProblemDetail(probe);
      if (detail) lines.push(detail);
    } else if (probe.verdict === "inconclusive") {
      lines.push(describeInconclusiveDetail(probe));
    }
  }

  lines.push("");
  lines.push(
    quietNow
      ? `Зараз тихі години (${formatQuietHoursLabel()}): перевірки тривають, але сповіщення не надсилаються.`
      : `Сповіщення активні. Тихі години: ${formatQuietHoursLabel()}.`,
  );
  const nextProbeAtMs = (lastScheduledCycleAtMs ?? Date.now()) + config.probeIntervalMs;
  lines.push(
    nextProbeAtMs <= Date.now()
      ? "Наступна перевірка ось-ось почнеться."
      : `Наступна перевірка о ${formatTimeShort(nextProbeAtMs)}.`,
  );
  lines.push(`Uptime: ${formatDuration(now - startedAt)}.`);

  return lines.join("\n");
}

function formatDayMessage() {
  const entries = getProbesSince(Date.now() - 24 * 3_600_000);

  if (entries.length === 0) {
    return "За останні 24 години перевірок ще не було.";
  }

  const byVerdict = { ok: 0, degraded: 0, down: 0, inconclusive: 0 };
  for (const entry of entries) byVerdict[entry.verdict] = (byVerdict[entry.verdict] || 0) + 1;

  const lines = ["<b>Стрічка угод за останні 24 години</b>", ""];
  const problems = byVerdict.degraded + byVerdict.down;

  if (problems === 0) {
    const quietChecks = entries.filter((entry) => entry.note === "quiet_market").length;
    const krakenFailures = entries.filter((entry) => isKrakenReferenceFailure(entry)).length;
    const notes = [];
    if (quietChecks > 0) notes.push(`${quietChecks} ${checksWord(quietChecks)} припали на тихий ринок.`);
    if (krakenFailures > 0) notes.push(`${krakenFailures} ${checksWord(krakenFailures)} не дали результату через Kraken.`);
    const headline = byVerdict.ok > 0
      ? `🟢 Проблем не було: сервер працював нормально всі ${byVerdict.ok} ${checksWord(byVerdict.ok)}.`
      : "⚪ Стан сервера за цей період підтвердити не вдалося.";
    lines.push([headline, ...notes].join(" "));
  } else {
    lines.push(`Перевірок: ${entries.length}, з них із проблемами: ${problems}.`);
  }

  // One line per probe, each with its persistent number — /details <№>
  // shows the full breakdown of that probe.
  lines.push("");
  const shown = entries.slice(-48);
  if (shown.length < entries.length) {
    lines.push(`(показані останні ${shown.length} перевірок)`);
  }
  for (const entry of shown) {
    let line = `№${entry.id} ${formatTimeShort(Date.parse(entry.at))} ${verdictEmoji(entry.verdict)}`;
    const summary = probeProblemSummary(entry);
    if (summary) line += ` ${summary}`;
    lines.push(line);
  }
  lines.push("", "Подробиці будь-якої перевірки: /details номер.");

  const delays = entries.map((entry) => entry.delayMedianMs).filter((value) => value !== null && value !== undefined);
  if (delays.length > 0) {
    const worst = Math.max(...entries.map((entry) => entry.delayMaxMs ?? 0));
    delays.sort((lhs, rhs) => lhs - rhs);
    lines.push(
      "",
      `Швидкість за добу: зазвичай угода долітала за ${formatDelay(delays[Math.floor(delays.length / 2)])}, у найгіршому випадку — за ${formatDelay(worst)}.`,
    );
  }

  return lines.join("\n");
}

function describeVerdictLine(probe) {
  switch (probe.verdict) {
    case "ok":
      return "🟢 сервер працює нормально.";
    case "degraded":
      return "🟠 сервер працює з перебоями (частина угод губиться або запізнюється).";
    case "down":
      return probe.note === "connect_failed"
        ? "🔴 сервер недоступний (не вдалося підключитися)."
        : "🔴 сервер не передає угоди.";
    default:
      if (probe.note === "kraken_disconnected") return "⚪ стан невідомий: Kraken розірвав з’єднання.";
      if (probe.note === "kraken_unavailable") return "⚪ стан невідомий: Kraken недоступний.";
      if (probe.note === "kraken_parse_failure") return "⚪ стан невідомий: повідомлення Kraken не вдалося розібрати.";
      return "⚪ ринок був надто тихий, щоб оцінити (мало угод на біржі).";
  }
}

// Short "what was wrong" for one probe in the /day list.
function probeProblemSummary(entry) {
  switch (entry.note) {
    case "connect_failed":
      return "сервер не відповідав";
    case "feed_silent":
    case "no_matches":
      return "угоди не доходили";
    case "invalid_feed_messages":
      return "фід надсилав пошкоджені дані";
    case "missing_trades":
      return `губилися угоди (дійшло ${entry.coveragePct}%)`;
    case "slow_delivery":
      return `затримки до ${formatDelay(entry.delaySlowMs ?? entry.delayMaxMs)}`;
    case "socket_dropped":
      return "обривалося з'єднання";
    case "kraken_disconnected":
      return "Kraken розірвав з’єднання";
    case "kraken_unavailable":
      return "Kraken був недоступний";
    case "kraken_parse_failure":
      return "повідомлення Kraken не розібралося";
    case "quiet_market":
      return "тихий ринок";
    default:
      return null;
  }
}

function verdictEmoji(verdict) {
  switch (verdict) {
    case "ok":
      return "🟢";
    case "degraded":
      return "🟠";
    case "down":
      return "🔴";
    default:
      return "⚪";
  }
}

// Trades that count toward coverage: exchange trades old enough to have had
// time to arrive. All user-facing numbers use this count so they always add up.
function referenceCount(probe) {
  return probe.referenceTrades ?? probe.matched + missingCount(probe);
}

function missingCount(probe) {
  if (probe.coveragePct === null || probe.coveragePct === 0) return probe.krakenTrades;
  return Math.max(0, Math.round((probe.matched * 100) / probe.coveragePct) - probe.matched);
}

function formatDelay(ms) {
  if (ms === null || ms === undefined) return "невідомо";
  if (ms < 10) return "менш ніж 0,01 с";
  if (ms < 1000) return `${(ms / 1000).toFixed(2).replace(".", ",")} с`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(1).replace(".", ",")} с`;
  return `${Math.round(ms / 1000)} с`;
}

function checksWord(count) {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return "перевірку";
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return "перевірки";
  return "перевірок";
}

function tradesWord(count) {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return "угоду";
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return "угоди";
  return "угод";
}

// --- telegram --------------------------------------------------------------

async function sendDetailsMessages(chatId, probe) {
  for (const message of formatDetailsMessages(probe)) {
    await sendTelegramMessage(chatId, message);
  }
}

async function sendTelegramMessage(chatId, text) {
  if (config.dryRun) {
    console.log(`[DRY_RUN] Telegram -> ${chatId}:\n${text}\n`);
    return;
  }

  const response = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: "HTML",
      disable_web_page_preview: true,
    }),
  });

  if (!response.ok) {
    throw new Error(`Telegram ${response.status}: ${await response.text()}`);
  }
}

async function pollTelegramCommands() {
  const params = new URLSearchParams({
    timeout: "0",
    allowed_updates: JSON.stringify(["message"]),
  });

  const savedOffset = kvGet("telegramUpdateOffset");
  if (savedOffset) {
    params.set("offset", String(savedOffset));
  }

  const response = await fetch(`https://api.telegram.org/bot${config.telegramBotToken}/getUpdates?${params}`);
  if (!response.ok) {
    throw new Error(`Telegram getUpdates ${response.status}: ${await response.text()}`);
  }

  const payload = await response.json();
  if (!payload.ok) {
    throw new Error(`Telegram getUpdates failed: ${JSON.stringify(payload)}`);
  }

  for (const update of payload.result || []) {
    kvSet("telegramUpdateOffset", update.update_id + 1);
    const message = update.message;
    const chatId = message?.chat?.id;
    const text = message?.text?.trim();

    if (!chatId || String(chatId) !== String(config.telegramChatId) || !text) {
      continue;
    }

    const parts = text.split(/\s+/);
    const command = parts[0].split("@")[0].toLowerCase();
    if (command === "/stats" || command === "stats") {
      await sendTelegramMessage(chatId, formatStatsMessage());
    } else if (command === "/day" || command === "day") {
      await sendTelegramMessage(chatId, formatDayMessage());
    } else if (command === "/details" || command === "details" || command === "/detail" || command === "detail") {
      const argument = (parts[1] || "").replace(/^[№#]/, "");
      if (!argument) {
        const probe = withTrades(getLastProbe());
        if (probe) {
          await sendDetailsMessages(chatId, probe);
        } else {
          await sendTelegramMessage(chatId, "Перевірок ще не було — деталей поки немає.");
        }
      } else {
        const id = Number(argument);
        const found = Number.isInteger(id) ? withTrades(getProbeById(id)) : null;
        if (found) {
          await sendDetailsMessages(chatId, found);
        } else {
          await sendTelegramMessage(chatId, `Не знайшов перевірку №${argument} — актуальні номери є в /day.`);
        }
      }
    } else if (command === "/check" || command === "check") {
      // Deliberately not awaited: command polling must stay responsive during
      // the whole probe. The activeProbe guard prevents a second probe.
      runManualCheck(chatId).catch((error) => {
        console.error("Manual check failed:", error.message);
      });
    }
  }
}

// --- read-only HTTP API ----------------------------------------------------
// Exists so the collected history can be inspected from outside Telegram
// (long-term patterns are easier to see with SQL than with /day). Every
// endpoint is read-only: the writable `db` handle is never used here, queries
// go through a separate connection opened with readOnly.

function getReadOnlyDb() {
  if (!readOnlyDb) {
    readOnlyDb = new DatabaseSync(config.dbFile, { readOnly: true });
  }
  return readOnlyDb;
}

function startApiServer() {
  if (!config.apiToken) {
    console.log("API disabled (no API_TOKEN set).");
    return;
  }
  if (!Number.isInteger(config.apiPort) || config.apiPort <= 0) {
    console.log(`API disabled (invalid port: ${process.env.PORT || process.env.API_PORT}).`);
    return;
  }

  const server = http.createServer((request, response) => {
    handleApiRequest(request, response).catch((error) => {
      sendJson(response, 500, { error: error.message });
    });
  });

  server.on("error", (error) => {
    console.error("API server error:", error.message);
  });

  server.listen(config.apiPort, () => {
    console.log(`API listening on port ${config.apiPort}.`);
  });
}

async function handleApiRequest(request, response) {
  const url = new URL(request.url, "http://localhost");
  const route = url.pathname.replace(/\/+$/, "") || "/";

  // Unauthenticated liveness probe for Railway's healthcheck.
  if (route === "/health") {
    const last = getLastProbe();
    sendJson(response, 200, {
      ok: true,
      uptimeSeconds: Math.round((Date.now() - startedAt.getTime()) / 1000),
      lastProbeAt: last?.at ?? null,
      lastVerdict: last?.verdict ?? null,
    });
    return;
  }

  if (!isAuthorized(request, url)) {
    sendJson(response, 401, { error: "unauthorized" });
    return;
  }

  if (route === "/api/stats") {
    const last = getLastProbe();
    const counts = getReadOnlyDb()
      .prepare("SELECT verdict, COUNT(*) AS count FROM probes GROUP BY verdict")
      .all();
    const span = getReadOnlyDb()
      .prepare("SELECT MIN(at) AS first, MAX(at) AS last, COUNT(*) AS total FROM probes")
      .get();
    sendJson(response, 200, {
      now: new Date().toISOString(),
      uptimeSeconds: Math.round((Date.now() - startedAt.getTime()) / 1000),
      quietHours: { active: isQuietHours(), label: formatQuietHoursLabel(), timeZone: config.quietHoursTimeZone },
      probeRunning: activeProbe !== null,
      nextProbeAt: new Date((lastScheduledCycleAtMs ?? Date.now()) + config.probeIntervalMs).toISOString(),
      history: { total: span?.total ?? 0, first: span?.first ?? null, last: span?.last ?? null },
      verdictCounts: Object.fromEntries(counts.map((row) => [row.verdict, row.count])),
      lastProbe: last,
      config: {
        feedUrl: config.feedUrl,
        feedChannel: config.feedChannel,
        krakenSymbol: config.krakenSymbol,
        probeIntervalMinutes: Math.round(config.probeIntervalMs / 60_000),
        probeWarmupSeconds: Math.round(config.probeWarmupMs / 1000),
        probePreRollSeconds: Math.round(config.probePreRollMs / 1000),
        probeWindowSeconds: Math.round(config.probeWindowMs / 1000),
        probeDrainSeconds: Math.round(config.probeDrainMs / 1000),
        slowDelayMs: thresholds.slowDelayMs,
      },
    });
    return;
  }

  const probeMatch = /^\/api\/probes\/(\d+)$/.exec(route);
  if (probeMatch) {
    const probe = withTrades(getProbeById(Number(probeMatch[1])));
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
      parameters.push(new Date(Date.now() - clampInt(hours, 24, 1, 24 * 365) * 3_600_000).toISOString());
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
    const rows = getReadOnlyDb()
      .prepare(`SELECT * FROM probes ${where} ORDER BY id DESC LIMIT ?`)
      .all(...parameters, limit)
      .map(rowToProbe);
    sendJson(response, 200, { count: rows.length, probes: rows });
    return;
  }

  if (route === "/api/sql") {
    const sql = url.searchParams.get("q") || (request.method === "POST" ? await readBody(request) : "");
    runReadOnlyQuery(response, sql);
    return;
  }

  sendJson(response, 404, {
    error: "unknown route",
    routes: ["/health", "/api/stats", "/api/probes", "/api/probes/:id", "/api/sql?q=SELECT..."],
  });
}

// Arbitrary SELECTs are the point of this API — ad-hoc questions about the
// history ("which hours degrade?") are not worth a new endpoint each. The
// connection is read-only, so the guards below only need to keep the query
// itself from being a statement batch or an unbounded dump.
function runReadOnlyQuery(response, rawSql) {
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
    const startedMs = Date.now();
    const rows = getReadOnlyDb().prepare(sql).all();
    sendJson(response, 200, {
      rowCount: rows.length,
      truncated: rows.length > SQL_ROW_LIMIT,
      elapsedMs: Date.now() - startedMs,
      rows: rows.slice(0, SQL_ROW_LIMIT),
    });
  } catch (error) {
    sendJson(response, 400, { error: error.message });
  }
}

function isAuthorized(request, url) {
  const header = request.headers.authorization || "";
  const provided = header.startsWith("Bearer ") ? header.slice(7) : url.searchParams.get("token") || "";
  const expected = Buffer.from(config.apiToken);
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

function sendJson(response, status, payload) {
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

// --- storage location and env ----------------------------------------------

function resolveDbFile() {
  if (process.env.DB_FILE) {
    return path.resolve(__dirname, process.env.DB_FILE);
  }

  const stateDir = process.env.STATE_DIR
    ? path.resolve(process.env.STATE_DIR)
    : process.env.RAILWAY_VOLUME_MOUNT_PATH
      ? path.resolve(process.env.RAILWAY_VOLUME_MOUNT_PATH)
    : __dirname;

  fs.mkdirSync(stateDir, { recursive: true });
  return path.join(stateDir, "heartbeat.db");
}

function loadLocalEnv(envPath) {
  if (!fs.existsSync(envPath)) {
    return;
  }

  for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    let value = trimmed.slice(separatorIndex + 1).trim();

    if (
      (value.startsWith("\"") && value.endsWith("\"")) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    process.env[key] ??= value;
  }
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

// --- quiet hours and formatting (mirrors app_store/bot.mjs) ----------------

function parseHour(value, envName) {
  const match = /^(\d{1,2}):([0-5]\d)$/.exec(value);
  if (!match) {
    throw new Error(`Invalid ${envName}: ${value}. Expected HH:mm.`);
  }

  const hour = Number(match[1]);
  const minute = Number(match[2]);

  if (hour > 23) {
    throw new Error(`Invalid ${envName}: ${value}. Hour must be between 0 and 23.`);
  }

  return hour + minute / 60;
}

function isQuietHours(now = new Date()) {
  const currentHour = getHourInTimeZone(now, config.quietHoursTimeZone);
  const startHour = config.quietHoursStartHour;
  const endHour = config.quietHoursEndHour;

  if (startHour === endHour) {
    return false;
  }

  if (startHour < endHour) {
    return currentHour >= startHour && currentHour < endHour;
  }

  return currentHour >= startHour || currentHour < endHour;
}

function getHourInTimeZone(date, timeZone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const hour = Number(parts.find((part) => part.type === "hour")?.value || 0);
  const minute = Number(parts.find((part) => part.type === "minute")?.value || 0);

  return hour + minute / 60;
}

function formatQuietHoursLabel() {
  return `${formatHour(config.quietHoursStartHour)}-${formatHour(config.quietHoursEndHour)} ${config.quietHoursTimeZone}`;
}

function formatHour(hourValue) {
  const totalMinutes = Math.round(hourValue * 60);
  const hours = String(Math.floor(totalMinutes / 60)).padStart(2, "0");
  const minutes = String(totalMinutes % 60).padStart(2, "0");
  return `${hours}:${minutes}`;
}

function formatTimeWithSeconds(ms) {
  return new Intl.DateTimeFormat("uk-UA", {
    timeZone: config.quietHoursTimeZone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(ms));
}

function formatTimeShort(ms) {
  return new Intl.DateTimeFormat("uk-UA", {
    timeZone: config.quietHoursTimeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(ms));
}

function formatDateTime(value) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: config.quietHoursTimeZone,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function formatDuration(durationMs) {
  const totalSeconds = Math.floor(durationMs / 1000);
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    return `${days}d ${hours}h ${minutes}m`;
  }

  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }

  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }

  return `${seconds}s`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

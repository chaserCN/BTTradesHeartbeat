import fs from "node:fs";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

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
};

// Degradation thresholds. Healthy baseline measured 2026-07-19: the feed relays
// Kraken trades with a typical delay of ~7ms and 100% coverage, so these are
// generous — anything beyond them means users actually feel the problem.
const thresholds = {
  slowDelayMs: 5_000, // slowest matched trades later than this => degraded
};

const expectedProbeDurationMs =
  (2 * config.probeWarmupMs) + (2 * config.connectTimeoutMs) + config.probeWindowMs + config.probeDrainMs;

// Storage: SQLite (built into Node, no dependencies). Two data tables — one
// row per probe ("проверка"), one row per lost trade ("сделка") — plus a
// small kv table for service state. Probe ids are permanent, so /day labels
// and /details <№> keep working forever; history is never trimmed.
const db = new DatabaseSync(config.dbFile);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    verdict TEXT NOT NULL,
    note TEXT,
    window_seconds INTEGER,
    handshake_ms INTEGER,
    kraken_trades INTEGER,
    our_trades INTEGER,
    reference_trades INTEGER,
    matched INTEGER,
    coverage_pct INTEGER,
    delay_median_ms INTEGER,
    delay_slow_ms INTEGER,
    delay_max_ms INTEGER,
    feed_closes INTEGER,
    feed_errors INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_probes_at ON probes(at);
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    probe_id INTEGER NOT NULL REFERENCES probes(id),
    at_ms INTEGER NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    delay_ms INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_trades_probe ON trades(probe_id);
  CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
`);

// Earlier versions stored only lost trades in a lost_trades table; fold them
// into the unified trades table (delivered = 0) once and drop the old table.
if (db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'lost_trades'").get()) {
  db.exec(`
    INSERT INTO trades (probe_id, at_ms, price, quantity, delivered)
      SELECT probe_id, at_ms, price, quantity, 0 FROM lost_trades;
    DROP TABLE lost_trades;
  `);
}

importLegacyJsonState();

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
    `window: ${Math.round(config.probeWindowMs / 1000)}s, ` +
    `drain: ${Math.round(config.probeDrainMs / 1000)}s.` +
    `${config.dryRun ? " DRY_RUN: Telegram messages go to console." : ""}`,
);

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
// cross-matches trades by price+quantity and produces a verdict.
//
// The feed server never acknowledges subscriptions and silently ignores bad
// channels, so a quiet market and a dead feed look identical from our socket
// alone — Kraken's own stream is the reference that tells them apart.
async function runProbe() {
  const startedAtMs = Date.now();
  const session = await collectSession(config.probeWindowMs, config.probeWarmupMs, config.probeDrainMs);
  const metrics = computeMetrics(session);
  const verdict = judge(session, metrics);

  const probe = {
    at: new Date(startedAtMs).toISOString(),
    verdict: verdict.verdict,
    note: verdict.note,
    windowSeconds: Math.round(session.windowMs / 1000),
    handshakeMs: session.feed.handshakeMs,
    krakenTrades: session.kraken.trades.length,
    ourTrades: session.feed.trades.length,
    referenceTrades: metrics.referenceTrades,
    matched: metrics.matched,
    coveragePct: metrics.coveragePct,
    delayMedianMs: metrics.delayMedianMs,
    delaySlowMs: metrics.delaySlowMs,
    delayMaxMs: metrics.delayMaxMs,
    feedCloses: session.feed.closes,
    feedErrors: session.feed.errors,
    // Every reference trade with its delivered/lost status, kept for /details.
    trades: metrics.allTrades.map((trade) => ({
      atMs: trade.atMs,
      price: trade.price,
      quantity: trade.quantity,
      delivered: trade.delivered,
      delayMs: trade.delayMs,
    })),
  };
  probe.lostTrades = probe.trades.filter((trade) => !trade.delivered);

  console.log(`Probe: ${JSON.stringify({ ...probe, trades: probe.trades.length, lostTrades: probe.lostTrades.length })}`);
  return probe;
}

function collectSession(windowMs, warmupMs, drainMs) {
  return new Promise((resolve) => {
    const session = {
      windowMs: 0,
      startedAtMs: null,
      referenceEndedAtMs: null,
      endedAtMs: null,
      feed: { handshakeMs: null, connectFailed: false, trades: [], closes: 0, errors: 0 },
      kraken: { connected: false, connectFailed: false, disconnected: false, trades: [] },
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
      if (session.startedAtMs !== null) {
        session.windowMs = (session.referenceEndedAtMs ?? session.endedAtMs) - session.startedAtMs;
      }
      for (const socket of [feedSocket, krakenSocket]) {
        try { socket?.close(); } catch { /* already closed */ }
      }
      resolve(session);
    };

    const beginMeasurement = () => {
      if (finished || phase !== "syncing") return;

      // Both subscriptions are ready. Discard everything seen during setup so
      // the reference and feed start at the same boundary.
      session.feed.trades = [];
      session.kraken.trades = [];
      session.startedAtMs = Date.now();
      phase = "measuring";

      measurementTimer = setTimeout(() => {
        if (finished || phase !== "measuring") return;
        session.referenceEndedAtMs = Date.now();
        phase = "draining";

        // The reference window is now fixed. Stop Kraken and give only our
        // feed extra time to deliver trades emitted just before the boundary.
        try { krakenSocket?.close(); } catch { /* already closed */ }
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
        const message = parseKrakenMessage(event.data);
        if (message.subscription === "accepted" && phase === "connecting_kraken") {
          clearTimeout(krakenConnectTimer);
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
        if (phase === "measuring") {
          session.kraken.trades.push(...message.trades);
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
      feedSocket.send(JSON.stringify({ subscribe: config.feedChannel }));
      phase = "warming_feed";
      warmupTimer = setTimeout(connectKraken, warmupMs);
    };
    feedSocket.onmessage = (event) => {
      if (phase === "measuring" || phase === "draining") {
        const trade = parseFeedTrade(event.data);
        if (trade) session.feed.trades.push(trade);
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

function parseFeedTrade(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return null;
  }
  if (payload?.price == null || payload?.quantity == null || payload?.time == null) return null;
  const price = Number(payload.price);
  const quantity = Number(payload.quantity);
  if (!Number.isFinite(price) || !Number.isFinite(quantity)) return null;
  return { atMs: Date.now(), price, quantity };
}

function parseKrakenMessage(raw) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return { subscription: null, error: null, trades: [] };
  }

  if (payload?.method === "subscribe") {
    return {
      subscription: payload.success === true ? "accepted" : "rejected",
      error: payload.error || null,
      trades: [],
    };
  }

  if (payload?.channel !== "trade" || payload?.type !== "update" || !Array.isArray(payload.data)) {
    return { subscription: null, error: null, trades: [] };
  }

  const receivedAtMs = Date.now();
  const trades = payload.data
    .map((trade) => ({
      atMs: receivedAtMs,
      price: Number(trade.price),
      quantity: Number(trade.qty),
    }))
    .filter((trade) => Number.isFinite(trade.price) && Number.isFinite(trade.quantity));
  return { subscription: null, error: null, trades };
}

function computeMetrics(session) {
  // Kraken stops at the reference boundary while our feed keeps draining, so
  // every reference trade has had the full delivery grace period.
  const reference = session.kraken.trades;

  const usedFeedIndices = new Set();
  const delays = [];
  const allTrades = [];
  for (const krakenTrade of reference) {
    const index = session.feed.trades.findIndex(
      (feedTrade, feedIndex) =>
        !usedFeedIndices.has(feedIndex) &&
        Math.abs(feedTrade.price - krakenTrade.price) < 1e-9 &&
        Math.abs(feedTrade.quantity - krakenTrade.quantity) < 1e-9,
    );
    if (index >= 0) {
      usedFeedIndices.add(index);
      const delayMs = Math.max(0, session.feed.trades[index].atMs - krakenTrade.atMs);
      delays.push(delayMs);
      allTrades.push({ ...krakenTrade, delivered: true, delayMs });
    } else {
      allTrades.push({ ...krakenTrade, delivered: false, delayMs: null });
    }
  }

  delays.sort((lhs, rhs) => lhs - rhs);
  return {
    referenceTrades: reference.length,
    allTrades,
    matched: delays.length,
    coveragePct: reference.length ? Math.round((delays.length / reference.length) * 100) : null,
    delayMedianMs: delays.length ? delays[Math.floor(delays.length / 2)] : null,
    delaySlowMs: delays.length ? delays[Math.min(delays.length - 1, Math.floor(delays.length * 0.9))] : null,
    delayMaxMs: delays.length ? delays[delays.length - 1] : null,
  };
}

function judge(session, metrics) {
  if (session.feed.connectFailed) {
    return { verdict: "down", note: "connect_failed" };
  }
  if (session.kraken.disconnected) {
    // A partial reference can hide trades lost by our feed, so never judge
    // against it even if Kraken delivered a few trades before disconnecting.
    return { verdict: "inconclusive", note: "kraken_disconnected" };
  }
  if (session.kraken.connectFailed) {
    // Cannot judge without the reference. Do not blame our feed.
    return { verdict: "inconclusive", note: "kraken_unavailable" };
  }
  if (session.kraken.trades.length === 0) {
    return { verdict: "inconclusive", note: "quiet_market" };
  }
  if (session.feed.trades.length === 0) {
    return { verdict: "down", note: "feed_silent" };
  }
  if (metrics.matched === 0) {
    return { verdict: "down", note: "no_matches" };
  }
  if (session.feed.closes > 0) {
    return { verdict: "degraded", note: "socket_dropped" };
  }
  if (metrics.matched < metrics.referenceTrades) {
    return { verdict: "degraded", note: "missing_trades" };
  }
  if (metrics.delaySlowMs !== null && metrics.delaySlowMs > thresholds.slowDelayMs) {
    return { verdict: "degraded", note: "slow_delivery" };
  }
  return { verdict: "ok", note: "" };
}

function recordProbe(probe) {
  const info = db.prepare(`
    INSERT INTO probes (
      at, verdict, note, window_seconds, handshake_ms, kraken_trades, our_trades,
      reference_trades, matched, coverage_pct, delay_median_ms, delay_slow_ms,
      delay_max_ms, feed_closes, feed_errors
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    probe.at, probe.verdict, probe.note ?? null, probe.windowSeconds ?? null,
    probe.handshakeMs ?? null, probe.krakenTrades ?? null, probe.ourTrades ?? null,
    probe.referenceTrades ?? null, probe.matched ?? null, probe.coveragePct ?? null,
    probe.delayMedianMs ?? null, probe.delaySlowMs ?? null, probe.delayMaxMs ?? null,
    probe.feedCloses ?? null, probe.feedErrors ?? null,
  );
  probe.id = Number(info.lastInsertRowid);
  insertTrades(probe.id, probe.trades || []);
}

function insertTrades(probeId, trades) {
  const statement = db.prepare(
    "INSERT INTO trades (probe_id, at_ms, price, quantity, delivered, delay_ms) VALUES (?, ?, ?, ?, ?, ?)",
  );
  for (const trade of trades) {
    statement.run(probeId, trade.atMs, trade.price, trade.quantity, trade.delivered ? 1 : 0, trade.delayMs ?? null);
  }
}

function rowToProbe(row) {
  if (!row) return null;
  return {
    id: row.id,
    at: row.at,
    verdict: row.verdict,
    note: row.note ?? "",
    windowSeconds: row.window_seconds,
    handshakeMs: row.handshake_ms,
    krakenTrades: row.kraken_trades,
    ourTrades: row.our_trades,
    referenceTrades: row.reference_trades,
    matched: row.matched,
    coveragePct: row.coverage_pct,
    delayMedianMs: row.delay_median_ms,
    delaySlowMs: row.delay_slow_ms,
    delayMaxMs: row.delay_max_ms,
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

function getTrades(probeId) {
  return db
    .prepare("SELECT at_ms, price, quantity, delivered, delay_ms FROM trades WHERE probe_id = ? ORDER BY at_ms, id")
    .all(probeId)
    .map((row) => ({
      atMs: row.at_ms,
      price: row.price,
      quantity: row.quantity,
      delivered: row.delivered === 1,
      delayMs: row.delay_ms,
    }));
}

function withTrades(probe) {
  if (!probe) return null;
  const trades = getTrades(probe.id);
  return { ...probe, trades, lostTrades: trades.filter((trade) => !trade.delivered) };
}

function kvGet(key) {
  return db.prepare("SELECT value FROM kv WHERE key = ?").get(key)?.value;
}

function kvSet(key, value) {
  db.prepare(
    "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
  ).run(key, String(value));
}

// One-time import of the pre-SQLite JSON state file, preserving probe numbers.
// The JSON file is renamed afterwards so the import never runs twice.
function importLegacyJsonState() {
  const legacyFile = path.join(path.dirname(config.dbFile), "heartbeat_state.json");
  if (!fs.existsSync(legacyFile)) return;

  let legacy;
  try {
    legacy = JSON.parse(fs.readFileSync(legacyFile, "utf8"));
  } catch (error) {
    console.error(`Cannot parse legacy state file, skipping import: ${error.message}`);
    return;
  }

  const hasProbes = db.prepare("SELECT id FROM probes LIMIT 1").get();
  if (!hasProbes) {
    const insert = db.prepare(`
      INSERT INTO probes (
        id, at, verdict, note, window_seconds, handshake_ms, kraken_trades, our_trades,
        reference_trades, matched, coverage_pct, delay_median_ms, delay_slow_ms,
        delay_max_ms, feed_closes, feed_errors
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const entry of legacy.history || []) {
      insert.run(
        entry.id ?? null, entry.at, entry.verdict, entry.note ?? null,
        entry.windowSeconds ?? null, entry.handshakeMs ?? null, entry.krakenTrades ?? null,
        entry.ourTrades ?? null, entry.referenceTrades ?? null, entry.matched ?? null,
        entry.coveragePct ?? null, entry.delayMedianMs ?? null, entry.delaySlowMs ?? null,
        entry.delayMaxMs ?? null, entry.feedCloses ?? null, entry.feedErrors ?? null,
      );
      if (entry.id != null && Array.isArray(entry.lostTrades)) {
        insertTrades(entry.id, entry.lostTrades.map((trade) => ({ ...trade, delivered: false })));
      }
    }
    if (legacy.telegramUpdateOffset != null) kvSet("telegramUpdateOffset", legacy.telegramUpdateOffset);
    if (legacy.lastNotifiedVerdict) kvSet("lastNotifiedVerdict", legacy.lastNotifiedVerdict);
    console.log(`Imported ${(legacy.history || []).length} probes from legacy JSON state.`);
  }

  fs.renameSync(legacyFile, `${legacyFile}.imported`);
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
  return probe.note === "kraken_unavailable" || probe.note === "kraken_disconnected";
}

function describeInconclusiveDetail(probe) {
  if (probe.note === "kraken_disconnected") {
    return "Kraken розірвав з’єднання під час перевірки. Еталонний потік неповний, тому стан BitcoinTicker оцінити неможливо.";
  }
  if (probe.note === "kraken_unavailable") {
    return "Біржа Kraken була недоступна, тому стан BitcoinTicker оцінити неможливо.";
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
  const pattern = describeLossPattern(lost);

  if (!trades.some((trade) => trade.delivered)) {
    // Old records stored only the lost trades — show what survived.
    lines.push("", "<b>Загублені угоди</b> (час · ціна · обсяг)");
    const shown = lost.slice(0, 10);
    const priceWidth = Math.max(...shown.map((trade) => String(trade.price).length));
    for (const trade of shown) {
      lines.push(`<code>${formatTimeWithSeconds(trade.atMs)}  ${String(trade.price).padStart(priceWidth)}  ${trade.quantity}</code>`);
    }
    if (lost.length > shown.length) {
      lines.push(`…і ще ${lost.length - shown.length}.`);
    }
    lines.push("", pattern);
    return [lines.join("\n")];
  }

  lines.push("", "<b>Угоди</b> (✓ дійшла · ✗ загублена)");
  const priceWidth = Math.max(...trades.map((trade) => String(trade.price).length));
  const rows = trades.map((trade) =>
    `${trade.delivered ? "✓" : "✗"} ${formatTimeWithSeconds(trade.atMs)}  ${String(trade.price).padStart(priceWidth)}  ${trade.quantity}`,
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
  if (probe.verdict === "inconclusive") return "ринок був надто тихий, щоб оцінити";
  const label = {
    connect_failed: "сервер не відповідав",
    feed_silent: "угоди не доходили",
    no_matches: "дані не збігалися з біржею",
    missing_trades: "частина угод губилася",
    slow_delivery: "угоди доходили із запізненням",
    socket_dropped: "сервер обривав з'єднання",
  }[probe.note];
  return label || (probe.verdict === "down" ? "сервер не працював" : "сервер працював з перебоями");
}

// Were the losses bursts (buffer stall) or spread out (overloaded fan-out)?
function describeLossPattern(lost) {
  if (lost.length < 3) {
    return "Втрат замало, щоб судити про закономірність.";
  }
  const bursts = [];
  let current = [lost[0]];
  for (let index = 1; index < lost.length; index += 1) {
    if (lost[index].atMs - lost[index - 1].atMs <= 2000) {
      current.push(lost[index]);
    } else {
      bursts.push(current);
      current = [lost[index]];
    }
  }
  bursts.push(current);

  const bigBursts = bursts.filter((burst) => burst.length >= 3);
  if (bigBursts.length === 1) {
    return `Втрати йшли пачкою з ${bigBursts[0].length} угод підряд — схоже на затик буфера на сервері.`;
  }
  if (bigBursts.length > 1) {
    const sizes = bigBursts.map((burst) => burst.length).join(", ");
    return `Втрати йшли пачками (розміри груп: ${sizes}) — схоже на затик буфера на сервері.`;
  }
  return "Втрати розкидані по всьому вікну поодинці — схоже на перевантажену розсилку, а не на затик буфера.";
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

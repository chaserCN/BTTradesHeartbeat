import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { judgeProbe } from "./heartbeat-core.mjs";
import { createApiHandler, sendJson } from "./heartbeat-api.mjs";
import { formatDetailsMessages } from "./heartbeat-format.mjs";
import { collectSession, computeSessionMetrics } from "./heartbeat-session.mjs";
import { createHeartbeatStore } from "./heartbeat-storage.mjs";

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

const store = createHeartbeatStore(config.dbFile);
const {
  getLastProbe,
  getProbeById,
  getProbesSince,
  kvGet,
  kvSet,
  recordProbe,
  withTrades,
} = store;

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
// cross-matches trades by price + quantity + side, bounded by exchange and
// monotonic receive times. Receive times remain separate from exchange times.
//
// The feed server never acknowledges subscriptions and silently ignores bad
// channels, so a quiet market and a dead feed look identical from our socket
// alone — Kraken's own stream is the reference that tells them apart.
async function runProbe() {
  const startedAtMs = Date.now();
  const session = await collectSession({
    windowMs: config.probeWindowMs,
    warmupMs: config.probeWarmupMs,
    drainMs: config.probeDrainMs,
    connectTimeoutMs: config.connectTimeoutMs,
    feedUrl: config.feedUrl,
    feedChannel: config.feedChannel,
    krakenUrl: config.krakenUrl,
    krakenSymbol: config.krakenSymbol,
  });
  const metrics = computeSessionMetrics(session, {
    preRollMs: config.probePreRollMs,
    drainMs: config.probeDrainMs,
  });
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
  for (const message of formatDetailsMessages(probe, {
    timeZone: config.quietHoursTimeZone,
    describeInconclusive: describeInconclusiveDetail,
  })) {
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

function startApiServer() {
  if (!config.apiToken) {
    console.log("API disabled (no API_TOKEN set).");
    return;
  }
  if (!Number.isInteger(config.apiPort) || config.apiPort <= 0) {
    console.log(`API disabled (invalid port: ${process.env.PORT || process.env.API_PORT}).`);
    return;
  }

  const handleApiRequest = createApiHandler({
    apiToken: config.apiToken,
    store,
    startedAtMs: startedAt.getTime(),
    getStatsContext: () => ({
      quietHours: {
        active: isQuietHours(),
        label: formatQuietHoursLabel(),
        timeZone: config.quietHoursTimeZone,
      },
      probeRunning: activeProbe !== null,
      nextProbeAt: new Date(
        (lastScheduledCycleAtMs ?? Date.now()) + config.probeIntervalMs,
      ).toISOString(),
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
    }),
  });
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

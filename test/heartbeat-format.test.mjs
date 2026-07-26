import assert from "node:assert/strict";
import test from "node:test";

import {
  formatDayProbeLine,
  formatDetailsMessages,
  formatQuietHoursDigestMessage,
  TELEGRAM_MESSAGE_LIMIT,
} from "../heartbeat-format.mjs";

// Intent: /day must make each check comparable at a glance without turning a
// missing Kraken reference into the misleading result "0/0".
test("day lines show delivery coverage or an explicit missing-reference state", () => {
  assert.equal(
    formatDayProbeLine({
      id: 1,
      at: "2026-07-20T14:42:40Z",
      verdict: "ok",
      matched: 150,
      referenceTrades: 150,
      coveragePct: 100,
    }, { timeZone: "Europe/Kyiv" }),
    "№1 🟢 17:42 150/150",
  );
  assert.equal(
    formatDayProbeLine({
      id: 2,
      at: "2026-07-20T14:42:40Z",
      verdict: "degraded",
      matched: 78,
      referenceTrades: 100,
      coveragePct: 78,
    }, { timeZone: "Europe/Kyiv" }),
    "№2 🟠 17:42 78/100",
  );
  assert.equal(
    formatDayProbeLine({
      id: 3,
      at: "2026-07-20T14:42:40Z",
      verdict: "inconclusive",
      matched: 0,
      referenceTrades: 0,
      coveragePct: null,
    }, { timeZone: "Europe/Kyiv" }),
    "№3 ⚪ 17:42 —",
  );
});

// Intent: a long failed probe must reach Telegram without truncating or
// duplicating evidence, while every emitted message stays below 4096 chars.
test("details chunking preserves every trade exactly once under Telegram's limit", () => {
  const trades = Array.from({ length: 500 }, (_, index) => ({
    exchangeAtMs: Date.parse("2026-07-20T09:05:00Z") + index * 100,
    receivedAtMs: 20_000 + index,
    price: 60_000 + index,
    quantity: Number(`0.${String(index + 1).padStart(8, "0")}`),
    side: index % 2 === 0 ? "buy" : "sell",
    delivered: index % 3 !== 0,
  }));
  const probe = {
    id: 42,
    at: "2026-07-20T09:05:00Z",
    verdict: "degraded",
    note: "missing_trades",
    windowSeconds: 90,
    referenceTrades: trades.length,
    matched: trades.filter((trade) => trade.delivered).length,
    coveragePct: 67,
    delayMedianMs: 31,
    delayMaxMs: 59,
    handshakeMs: 10,
    subscribeToFirstTradeMs: 14,
    krakenSyncTrades: 3,
    syncMatched: 3,
    syncCoveragePct: 100,
    deliveryHorizonFeedParseFailures: 0,
    referenceWindowKrakenParseFailures: 0,
    feedCloses: 0,
    feedErrors: 0,
    trades,
    lostTrades: trades.filter((trade) => !trade.delivered),
  };

  const messages = formatDetailsMessages(probe, { timeZone: "UTC" });
  assert.ok(messages.length > 2);
  assert.ok(messages.every((message) => message.length <= TELEGRAM_MESSAGE_LIMIT));
  assert.ok(messages.every((message) => (message.match(/<pre>/g) || []).length === (message.match(/<\/pre>/g) || []).length));
  const output = messages.join("\n");
  assert.equal((output.match(/[✓✗] \d{2}:\d{2}:\d{2}/gu) || []).length, trades.length);
  for (const trade of trades) {
    assert.equal(countOccurrences(output, `  ${trade.price}  `), 1, `price ${trade.price}`);
  }
  assert.match(output, /Серії втрат|безперервну серію/);
});

test("row chunking does not drop a row at an exact boundary", async () => {
  const { chunkRowsByLength } = await import("../heartbeat-format.mjs");
  assert.deepEqual(chunkRowsByLength(["1234", "5678", "9"], 10), [["1234", "5678"], ["9"]]);
});

// Intent: pre-measurement telemetry must remain available to diagnostics
// without leaking implementation noise into the user-facing Telegram report.
test("details hide internal lifecycle telemetry", () => {
  const output = formatDetailsMessages({
    id: 8,
    at: "2026-07-20T09:05:00Z",
    verdict: "ok",
    note: "",
    windowSeconds: 90,
    referenceTrades: 4,
    matched: 4,
    coveragePct: 100,
    delayMedianMs: 31,
    delayMaxMs: 52,
    handshakeMs: 10,
    subscribeToFirstTradeMs: 14,
    krakenSyncTrades: 3,
    syncMatched: 2,
    syncCoveragePct: 67,
    deliveryHorizonFeedParseFailures: 0,
    referenceWindowKrakenParseFailures: 0,
    feedCloses: 0,
    feedErrors: 0,
    lostTrades: [],
  }, { timeZone: "UTC" }).join("\n");

  assert.doesNotMatch(output, /Підключення|Перша угода після підписки|Перекривний прогрів/);
  assert.match(output, /Загублених угод не було/);
});

// Intent: the loss summary is a nominative counted list, so a single loss
// must read "1 угода" rather than the accusative "1 угоду".
test("a single loss uses the nominative Ukrainian form", () => {
  const base = Date.parse("2026-07-20T09:05:00Z");
  const trades = [
    { exchangeAtMs: base, receivedAtMs: base + 30, price: 60_000, quantity: 0.001, side: "buy", delivered: true },
    { exchangeAtMs: base + 1_000, receivedAtMs: base + 1_030, price: 60_001, quantity: 0.002, side: "sell", delivered: false },
    { exchangeAtMs: base + 2_000, receivedAtMs: base + 2_030, price: 60_002, quantity: 0.003, side: "buy", delivered: true },
  ];
  const output = formatDetailsMessages({
    id: 9,
    at: "2026-07-20T09:05:00Z",
    verdict: "degraded",
    note: "missing_trades",
    windowSeconds: 90,
    referenceTrades: 3,
    matched: 2,
    coveragePct: 67,
    delayMedianMs: 30,
    delayMaxMs: 30,
    handshakeMs: 10,
    deliveryHorizonFeedParseFailures: 0,
    referenceWindowKrakenParseFailures: 0,
    feedCloses: 0,
    feedErrors: 0,
    trades,
    lostTrades: [trades[1]],
  }, { timeZone: "UTC" }).join("\n");

  assert.match(output, /1 угода посередині вікна/);
  assert.doesNotMatch(output, /1 угоду/);
});

test("every verdict detail variant is bounded and contains no missing placeholders", () => {
  const variants = [
    ["ok", ""],
    ["degraded", "missing_trades"],
    ["degraded", "invalid_feed_messages"],
    ["degraded", "socket_dropped"],
    ["down", "connect_failed"],
    ["down", "feed_silent"],
    ["down", "no_matches"],
    ["inconclusive", "quiet_market"],
    ["inconclusive", "kraken_unavailable"],
    ["inconclusive", "kraken_disconnected"],
    ["inconclusive", "kraken_parse_failure"],
  ];
  for (const [verdict, note] of variants) {
    const messages = formatDetailsMessages({
      id: 7,
      at: "2026-07-20T09:05:00Z",
      verdict,
      note,
      windowSeconds: 90,
      referenceTrades: 2,
      matched: verdict === "ok" ? 2 : 1,
      coveragePct: verdict === "ok" ? 100 : 50,
      delayMedianMs: 31,
      delayMaxMs: 59,
      handshakeMs: 10,
      subscribeToFirstTradeMs: 14,
      krakenSyncTrades: 1,
      syncMatched: 1,
      syncCoveragePct: 100,
      deliveryHorizonFeedParseFailures: note === "invalid_feed_messages" ? 1 : 0,
      referenceWindowKrakenParseFailures: note === "kraken_parse_failure" ? 1 : 0,
      feedCloses: note === "socket_dropped" ? 1 : 0,
      feedErrors: 0,
      lostTrades: [],
    }, { timeZone: "UTC" });
    assert.ok(messages.every((message) => message.length <= TELEGRAM_MESSAGE_LIMIT));
    assert.doesNotMatch(messages.join("\n"), /undefined|NaN/);
  }
});

// Intent: the morning digest is the only trace of an overnight problem, so it
// must name every affected check and say where things stand now.
test("the quiet-hours digest lists affected checks and the current state", () => {
  const digest = {
    total: 10,
    problems: [
      nightEntry(41, "2026-07-20T23:42:00Z", "degraded", 44, 61, "missing_trades"),
      nightEntry(43, "2026-07-21T01:42:00Z", "down", 0, 57, "feed_silent"),
    ],
    referenceFailures: [nightEntry(45, "2026-07-21T03:42:00Z", "inconclusive", 0, 0, "kraken_unavailable")],
    worstVerdict: "down",
  };

  const message = formatQuietHoursDigestMessage(digest, {
    timeZone: "UTC",
    quietHoursLabel: "23:00-09:00 Europe/Kyiv",
    currentStateLine: "🟢 сервер працює нормально.",
  });

  assert.ok(message.length <= TELEGRAM_MESSAGE_LIMIT);
  assert.doesNotMatch(message, /undefined|NaN/);
  assert.match(message, /Перевірок: 10, з них із проблемами: 2\./);
  assert.match(message, /Не вдалося перевірити через Kraken: 1\./);
  assert.match(message, /№41 🟠 23:42 44\/61 — губилися угоди/);
  assert.match(message, /№43 🔴 01:42 0\/57 — угоди не доходили/);
  assert.match(message, /№45 ⚪ 03:42 — Kraken був недоступний/);
  assert.doesNotMatch(message, /— —/, "the missing-reference dash doubles as the separator");
  assert.match(message, /Зараз: 🟢 сервер працює нормально\./);
  assert.doesNotMatch(message, /показано лише/);
});

// Intent: full coverage with slow delivery is the one problem the counts alone
// cannot show, so the reason must be there — with or without a measured delay.
test("the digest explains a problem the delivery counts hide", () => {
  const slow = nightEntry(52, "2026-07-21T02:42:00Z", "degraded", 52, 52, "slow_delivery");
  const measured = formatQuietHoursDigestMessage(
    { total: 6, problems: [{ ...slow, delayP90Ms: 8_200, delayMaxMs: 12_400 }], referenceFailures: [], worstVerdict: "degraded" },
    { timeZone: "UTC" },
  );
  assert.match(measured, /№52 🟠 02:42 52\/52 — затримки до 8,2 с/);

  const unmeasured = formatQuietHoursDigestMessage(
    { total: 6, problems: [{ ...slow, delayP90Ms: null, delayMaxMs: null }], referenceFailures: [], worstVerdict: "degraded" },
    { timeZone: "UTC" },
  );
  assert.match(unmeasured, /№52 🟠 02:42 52\/52 — угоди доходили із запізненням/);
  assert.doesNotMatch(unmeasured, /невідомо/);
});

// Intent: an unhealthy night can hold far more probes than a chat message can
// carry, and a dropped tail must be visible rather than silently truncated.
test("a long quiet period is capped with an explicit count and keeps the latest checks", () => {
  const problems = Array.from({ length: 40 }, (_, index) =>
    nightEntry(100 + index, `2026-07-21T0${index % 6}:0${index % 6}:00Z`, "degraded", 10, 20, "missing_trades"));
  const message = formatQuietHoursDigestMessage(
    { total: 60, problems, referenceFailures: [], worstVerdict: "degraded" },
    { timeZone: "UTC", maxProbeLines: 24 },
  );

  assert.match(message, /\(показано лише 24 із 40\)/);
  assert.equal((message.match(/^№\d+ /gmu) || []).length, 24);
  assert.match(message, /№139 /);
  assert.doesNotMatch(message, /№115 /);
  assert.ok(message.length <= TELEGRAM_MESSAGE_LIMIT);
});

// Intent: when the morning probe still fails, its own alert follows the digest,
// so the digest must not state the current verdict a second time.
test("the digest omits the current state when a verdict alert follows", () => {
  const message = formatQuietHoursDigestMessage(
    {
      total: 4,
      problems: [nightEntry(51, "2026-07-21T02:42:00Z", "down", 0, 30, "connect_failed")],
      referenceFailures: [],
      worstVerdict: "down",
    },
    { timeZone: "UTC", quietHoursLabel: "23:00-09:00 Europe/Kyiv" },
  );

  assert.doesNotMatch(message, /Зараз:/);
  assert.doesNotMatch(message, /Kraken/);
  assert.match(message, /Подробиці будь-якої перевірки: \/details номер\./);
});

function nightEntry(id, at, verdict, matched, referenceTrades, note = "") {
  return {
    id,
    at,
    verdict,
    note,
    matched,
    referenceTrades,
    coveragePct: referenceTrades === 0 ? null : Math.round((matched * 100) / referenceTrades),
  };
}

function countOccurrences(text, needle) {
  return text.split(needle).length - 1;
}

import assert from "node:assert/strict";
import test from "node:test";

import { formatDayProbeLine, formatDetailsMessages, TELEGRAM_MESSAGE_LIMIT } from "../heartbeat-format.mjs";

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
    "№1, 17:42: дійшло 150/150 (100%) 🟢",
  );
  assert.equal(
    formatDayProbeLine({
      id: 2,
      at: "2026-07-20T14:42:40Z",
      verdict: "degraded",
      matched: 78,
      referenceTrades: 100,
      coveragePct: 78,
    }, { timeZone: "Europe/Kyiv", problemSummary: "губилися угоди" }),
    "№2, 17:42: дійшло 78/100 (78%) 🟠 — губилися угоди",
  );
  assert.equal(
    formatDayProbeLine({
      id: 3,
      at: "2026-07-20T14:42:40Z",
      verdict: "inconclusive",
      matched: 0,
      referenceTrades: 0,
      coveragePct: null,
    }, { timeZone: "Europe/Kyiv", problemSummary: "тихий ринок" }),
    "№3, 17:42: немає еталонних угод ⚪ — тихий ринок",
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

function countOccurrences(text, needle) {
  return text.split(needle).length - 1;
}

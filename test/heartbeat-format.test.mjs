import assert from "node:assert/strict";
import test from "node:test";

import { formatDetailsMessages, TELEGRAM_MESSAGE_LIMIT } from "../heartbeat-format.mjs";

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
    measurementFeedParseFailures: 0,
    measurementKrakenParseFailures: 0,
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
      krakenTrades: 2,
      matched: verdict === "ok" ? 2 : 1,
      coveragePct: verdict === "ok" ? 100 : 50,
      delayMedianMs: 31,
      delayMaxMs: 59,
      handshakeMs: 10,
      subscribeToFirstTradeMs: 14,
      krakenSyncTrades: 1,
      syncMatched: 1,
      syncCoveragePct: 100,
      measurementFeedParseFailures: note === "invalid_feed_messages" ? 1 : 0,
      measurementKrakenParseFailures: note === "kraken_parse_failure" ? 1 : 0,
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

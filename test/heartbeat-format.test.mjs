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

function countOccurrences(text, needle) {
  return text.split(needle).length - 1;
}

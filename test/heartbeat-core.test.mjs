import assert from "node:assert/strict";
import test from "node:test";

import {
  analyzeLossRuns,
  computeTradeMetrics,
  judgeProbe,
  parseFeedTrade,
  parseKrakenMessage,
  tradeMatchKey,
} from "../heartbeat-core.mjs";

const baseExchangeAtMs = Date.parse("2026-07-20T09:05:22.194Z");

function krakenTrade(overrides = {}) {
  return {
    exchangeAtMs: baseExchangeAtMs,
    receivedAtMs: 10_000,
    receivedMonoMs: 1_000,
    price: 63_977.5,
    quantity: 0.00078153,
    side: "sell",
    tradeId: "1",
    ...overrides,
  };
}

function feedTrade(overrides = {}) {
  return {
    exchangeAtMs: Math.round(baseExchangeAtMs / 1_000) * 1_000,
    receivedAtMs: 10_031,
    receivedMonoMs: 1_031,
    price: 63_977.5,
    quantity: 0.00078153,
    side: "sell",
    sequence: 1,
    ...overrides,
  };
}

test("parsers retain exchange time, receive time, side, and Kraken trade id", () => {
  const feed = parseFeedTrade(
    JSON.stringify({ time: 1_784_538_322, price: "63977.50000", quantity: 0.00078153, type: "sell" }),
    20_000,
    2_000,
  );
  assert.equal(feed.parseFailures, 0);
  assert.deepEqual(feed.trade, {
    exchangeAtMs: 1_784_538_322_000,
    receivedAtMs: 20_000,
    receivedMonoMs: 2_000,
    price: 63_977.5,
    quantity: 0.00078153,
    side: "sell",
  });

  const kraken = parseKrakenMessage(
    JSON.stringify({
      channel: "trade",
      type: "update",
      data: [{
        timestamp: "2026-07-20T09:05:22.194771Z",
        price: 63_977.5,
        qty: 0.00078153,
        side: "sell",
        trade_id: 103984267,
      }],
    }),
    20_031,
    2_031,
  );
  assert.equal(kraken.parseFailures, 0);
  assert.ok(Math.abs(kraken.trades[0].exchangeAtMs - 1_784_538_322_194.771) < 0.001);
  assert.equal(kraken.trades[0].tradeId, "103984267");
  assert.equal(kraken.trades[0].side, "sell");
});

test("malformed feed messages are counted instead of disappearing", () => {
  assert.deepEqual(parseFeedTrade("not json", 1, 1), { trade: null, parseFailures: 1 });
  assert.deepEqual(
    parseFeedTrade(JSON.stringify({ time: 1, price: 2, quantity: 3 }), 1, 1),
    { trade: null, parseFailures: 1 },
  );
});

test("the real value grain stays outside the former numeric tolerance", () => {
  const reference = krakenTrade();
  assert.notEqual(tradeMatchKey(reference), tradeMatchKey({ ...reference, price: reference.price + 0.1 }));
  assert.notEqual(
    tradeMatchKey(reference),
    tradeMatchKey({ ...reference, quantity: reference.quantity + 0.00000001 }),
  );
});

test("a feed copy received before t0 can match its Kraken copy after t0", () => {
  const metrics = computeTradeMetrics(
    [krakenTrade({ receivedMonoMs: 2_010 })],
    [feedTrade({ receivedMonoMs: 1_990 })],
    { maxLeadMs: 2_000, maxLagMs: 10_000 },
  );
  assert.equal(metrics.matched, 1);
  assert.equal(metrics.allTrades[0].signedDelayMs, -20);
  assert.equal(metrics.allTrades[0].delayMs, 0);
});

test("side prevents unrelated equal-value matches", () => {
  const reference = krakenTrade();
  const wrongSide = feedTrade({ side: "buy" });
  const metrics = computeTradeMetrics([reference], [wrongSide]);
  assert.equal(metrics.matched, 0);
});

test("one-second feed time can cross the Kraken exchange-second boundary", () => {
  const reference = krakenTrade({ exchangeAtMs: baseExchangeAtMs + 301 }); // .495301
  const nextSecondFeed = feedTrade({
    exchangeAtMs: (Math.floor(reference.exchangeAtMs / 1_000) + 1) * 1_000,
    receivedMonoMs: reference.receivedMonoMs + 27,
  });
  const metrics = computeTradeMetrics([reference], [nextSecondFeed]);
  assert.equal(metrics.matched, 1);
});

test("sub-millisecond Kraken precision is retained at the feed rounding boundary", () => {
  const kraken = parseKrakenMessage(
    JSON.stringify({
      channel: "trade",
      type: "update",
      data: [{
        timestamp: "2026-07-20T09:05:22.499600Z",
        price: 63_977.5,
        qty: 0.00078153,
        side: "sell",
      }],
    }),
    1_000,
    1_000,
  ).trades[0];
  const feed = feedTrade({
    exchangeAtMs: Math.floor(baseExchangeAtMs / 1_000) * 1_000,
    receivedMonoMs: 1_010,
  });
  assert.ok(Math.abs((kraken.exchangeAtMs % 1_000) - 499.6) < 0.001);
  assert.equal(computeTradeMetrics([kraken], [feed]).matched, 1);
});

test("exchange time remains a broad bound against distant identical values", () => {
  const reference = krakenTrade();
  const distant = feedTrade({
    exchangeAtMs: reference.exchangeAtMs + 20_000,
    receivedMonoMs: reference.receivedMonoMs + 20,
  });
  assert.equal(computeTradeMetrics([reference], [distant]).matched, 0);
});

test("a same-key trade outside the receive-time window cannot backfill a loss", () => {
  const reference = krakenTrade({ receivedMonoMs: 1_000 });
  const muchLater = feedTrade({ receivedMonoMs: 12_000 });
  const metrics = computeTradeMetrics([reference], [muchLater], { maxLeadMs: 2_000, maxLagMs: 10_000 });
  assert.equal(metrics.matched, 0);
});

test("sync context reserves a pre-window feed copy from an in-window duplicate", () => {
  const syncReference = krakenTrade({ tradeId: "sync", receivedMonoMs: 990 });
  const measurementReference = krakenTrade({ tradeId: "measurement", receivedMonoMs: 1_020 });
  const syncFeedCopy = feedTrade({ sequence: 1, receivedMonoMs: 995 });
  const metrics = computeTradeMetrics([syncReference, measurementReference], [syncFeedCopy]);
  assert.equal(metrics.allTrades[0].delivered, true);
  assert.equal(metrics.allTrades[1].delivered, false);
});

test("drain context reserves a post-window feed copy from an in-window duplicate", () => {
  const measurementReference = krakenTrade({ tradeId: "measurement", receivedMonoMs: 1_000 });
  const drainReference = krakenTrade({ tradeId: "drain", receivedMonoMs: 1_100 });
  const drainFeedCopy = feedTrade({ sequence: 1, receivedMonoMs: 1_105 });
  const metrics = computeTradeMetrics([measurementReference, drainReference], [drainFeedCopy]);
  assert.equal(metrics.allTrades[0].delivered, false);
  assert.equal(metrics.allTrades[1].delivered, true);
});

test("matching maximizes cardinality before minimizing global time distance", () => {
  const references = [
    krakenTrade({ tradeId: "1", receivedMonoMs: 8 }),
    krakenTrade({ tradeId: "2", receivedMonoMs: 9 }),
  ];
  const feeds = [
    feedTrade({ sequence: 1, receivedMonoMs: 0 }),
    feedTrade({ sequence: 2, receivedMonoMs: 10 }),
  ];
  const metrics = computeTradeMetrics(references, feeds, { maxLeadMs: 20, maxLagMs: 20 });
  assert.equal(metrics.matched, 2);
  assert.equal(metrics.allTrades[0].matchedFeedSequence, 1);
  assert.equal(metrics.allTrades[1].matchedFeedSequence, 2);
  assert.deepEqual(metrics.allTrades.map((trade) => trade.signedDelayMs), [-8, 1]);
});

test("global matcher agrees with brute force on randomized small classes", () => {
  let state = 0x5eed1234;
  const random = (limit) => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state % limit;
  };

  for (let iteration = 0; iteration < 300; iteration += 1) {
    const references = Array.from({ length: 1 + random(6) }, (_, index) =>
      krakenTrade({ tradeId: String(index), receivedMonoMs: random(31) - 15 }));
    const feeds = Array.from({ length: 1 + random(6) }, (_, index) =>
      feedTrade({ sequence: index + 1, receivedMonoMs: random(31) - 15 }));
    const maxLeadMs = 10;
    const maxLagMs = 10;
    const metrics = computeTradeMetrics(references, feeds, { maxLeadMs, maxLagMs });
    const actualCost = metrics.allTrades
      .filter((trade) => trade.delivered)
      .reduce((sum, trade) => sum + Math.abs(trade.signedDelayMs), 0);
    const expected = bruteForceMatching(references, feeds, maxLeadMs, maxLagMs);
    assert.deepEqual(
      { matched: metrics.matched, cost: actualCost },
      expected,
      `iteration ${iteration}`,
    );
  }
});

test("delivered trades split loss runs even when timestamps are close", () => {
  const trades = [
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs }), delivered: false },
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs + 100 }), delivered: true },
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs + 200 }), delivered: false },
  ];
  const analysis = analyzeLossRuns(trades);
  assert.equal(analysis.runs.length, 2);
  assert.deepEqual(analysis.runs.map((run) => run.position), ["start", "end"]);
});

test("quiet gaps split loss runs and one- or two-trade runs are retained", () => {
  const trades = [
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs }), delivered: false },
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs + 100 }), delivered: false },
    { ...krakenTrade({ exchangeAtMs: baseExchangeAtMs + 3_000 }), delivered: false },
  ];
  const analysis = analyzeLossRuns(trades);
  assert.deepEqual(analysis.runs.map((run) => run.count), [2, 1]);
});

test("verdict distinguishes feed silence from malformed messages", () => {
  const session = {
    feed: { connectFailed: false, closes: 0 },
    kraken: { connectFailed: false, disconnected: false },
  };
  const baseMetrics = {
    referenceTrades: 2,
    feedCandidates: [],
    matched: 0,
    feedParseFailures: 0,
    krakenParseFailures: 0,
    delaySlowMs: null,
  };
  assert.deepEqual(judgeProbe(session, { ...baseMetrics, feedMessages: 0 }), {
    verdict: "down",
    note: "feed_silent",
  });
  assert.deepEqual(judgeProbe(session, { ...baseMetrics, feedMessages: 2, feedParseFailures: 2 }), {
    verdict: "down",
    note: "invalid_feed_messages",
  });
});

test("a malformed Kraken measurement makes the verdict inconclusive", () => {
  const session = {
    feed: { connectFailed: false, closes: 0 },
    kraken: { connectFailed: false, disconnected: false },
  };
  const verdict = judgeProbe(session, {
    referenceTrades: 3,
    feedCandidates: [feedTrade()],
    matched: 3,
    feedMessages: 1,
    feedParseFailures: 0,
    krakenParseFailures: 1,
    delaySlowMs: 10,
  });
  assert.deepEqual(verdict, { verdict: "inconclusive", note: "kraken_parse_failure" });
});

function bruteForceMatching(references, feeds, maxLeadMs, maxLagMs) {
  let best = { matched: -1, cost: Infinity };
  const visit = (referenceIndex, usedFeed, matched, cost) => {
    if (referenceIndex === references.length) {
      if (matched > best.matched || (matched === best.matched && cost < best.cost)) {
        best = { matched, cost };
      }
      return;
    }
    visit(referenceIndex + 1, usedFeed, matched, cost);
    for (let feedIndex = 0; feedIndex < feeds.length; feedIndex += 1) {
      if (usedFeed.has(feedIndex)) continue;
      const delay = feeds[feedIndex].receivedMonoMs - references[referenceIndex].receivedMonoMs;
      if (delay < -maxLeadMs || delay > maxLagMs) continue;
      usedFeed.add(feedIndex);
      visit(referenceIndex + 1, usedFeed, matched + 1, cost + Math.abs(Math.round(delay)));
      usedFeed.delete(feedIndex);
    }
  };
  visit(0, new Set(), 0, 0);
  return best;
}

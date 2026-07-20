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

// Intent: JSON coercion must not turn absent, empty, boolean, zero, or negative
// wire values into plausible trades that evade parse-failure telemetry.
test("parsers reject non-positive and coercible fake numeric fields", () => {
  const invalidValues = [null, "", "   ", "0x10", false, true, 0, -1];
  for (const value of invalidValues) {
    const feedPrice = parseFeedTrade(JSON.stringify({
      time: 1_784_538_322,
      price: value,
      quantity: 0.0001,
      type: "buy",
    }), 1, 1);
    const feedQuantity = parseFeedTrade(JSON.stringify({
      time: 1_784_538_322,
      price: 60_000,
      quantity: value,
      type: "buy",
    }), 1, 1);
    assert.equal(feedPrice.parseFailures, 1, `feed price ${JSON.stringify(value)}`);
    assert.equal(feedQuantity.parseFailures, 1, `feed quantity ${JSON.stringify(value)}`);

    const kraken = parseKrakenMessage(JSON.stringify({
      channel: "trade",
      type: "update",
      data: [{
        timestamp: "2026-07-20T09:05:22Z",
        price: value,
        qty: 0.0001,
        side: "buy",
      }],
    }), 1, 1);
    assert.equal(kraken.parseFailures, 1, `Kraken price ${JSON.stringify(value)}`);
    assert.equal(kraken.trades.length, 0);

    const krakenQuantity = parseKrakenMessage(JSON.stringify({
      channel: "trade",
      type: "update",
      data: [{
        timestamp: "2026-07-20T09:05:22Z",
        price: 60_000,
        qty: value,
        side: "buy",
      }],
    }), 1, 1);
    assert.equal(krakenQuantity.parseFailures, 1, `Kraken quantity ${JSON.stringify(value)}`);
  }
  assert.equal(parseFeedTrade(JSON.stringify({
    time: -1,
    price: 60_000,
    quantity: 0.0001,
    type: "buy",
  }), 1, 1).parseFailures, 1);
});

test("a mixed Kraken packet retains valid trades and counts each invalid item", () => {
  const parsed = parseKrakenMessage(JSON.stringify({
    channel: "trade",
    type: "update",
    data: [
      {
        timestamp: "2026-07-20T09:05:22.194771Z",
        price: 63_977.5,
        qty: 0.00078153,
        side: "sell",
        trade_id: 1,
      },
      { timestamp: "invalid", price: 63_977.5, qty: 0.00078153, side: "sell" },
      { timestamp: "2026-07-20T09:05:22Z", price: 63_977.5, qty: 0.00078153, side: "hold" },
    ],
  }), 20_000, 2_000);
  assert.equal(parsed.trades.length, 1);
  assert.equal(parsed.trades[0].tradeId, "1");
  assert.equal(parsed.parseFailures, 2);
});

test("malformed Kraken JSON is counted while non-trade control messages are ignored", () => {
  assert.deepEqual(parseKrakenMessage("{broken", 1, 1), {
    subscription: null,
    error: null,
    trades: [],
    parseFailures: 1,
  });
  assert.deepEqual(parseKrakenMessage(JSON.stringify({ channel: "heartbeat" }), 1, 1), {
    subscription: null,
    error: null,
    trades: [],
    parseFailures: 0,
  });
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

// Intent: across mixed keys and both temporal bounds, production must equal an
// independent exhaustive maximum-cardinality/minimum-cost oracle.
test("mixed-key matcher agrees with exhaustive oracle under randomized fractional timing", () => {
  let state = 0xc0ffee42;
  const random = (limit) => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state % limit;
  };
  const makeValue = (kind, index) => ({
    exchangeAtMs: baseExchangeAtMs + random(25) - 12 + (random(2) ? 0.5 : 0),
    receivedAtMs: 10_000 + index,
    receivedMonoMs: random(25) - 12 + (random(2) ? 0.5 : 0),
    price: 63_977.5 + random(3) * 0.1,
    quantity: 0.00000001 * (1 + random(3)),
    side: random(2) ? "buy" : "sell",
    ...(kind === "reference" ? { tradeId: String(index) } : { sequence: index + 1 }),
  });
  const limits = { maxLeadMs: 5, maxLagMs: 7, maxExchangeSkewMs: 6 };

  for (let iteration = 0; iteration < 300; iteration += 1) {
    const references = Array.from({ length: 1 + random(6) }, (_, index) => makeValue("reference", index));
    const feeds = Array.from({ length: 1 + random(6) }, (_, index) => makeValue("feed", index));
    const expected = bruteForceBoundedMatching(references, feeds, limits);
    const metrics = computeTradeMetrics(references, feeds, limits);
    const actualCost = metrics.allTrades.reduce((sum, result, referenceIndex) => {
      if (!result.delivered) return sum;
      const matchedFeed = feeds.find((feed) => feed.sequence === result.matchedFeedSequence);
      return sum + Math.round(Math.abs(matchedFeed.receivedMonoMs - references[referenceIndex].receivedMonoMs));
    }, 0);
    assert.deepEqual(
      { matched: metrics.matched, cost: actualCost, uniqueFeeds: metrics.matchedFeedIndices.size },
      { ...expected, uniqueFeeds: expected.matched },
      `seed iteration ${iteration}`,
    );

    const reversed = computeTradeMetrics([...references].reverse(), [...feeds].reverse(), limits);
    const reversedCost = reversed.allTrades.reduce((sum, result, reversedReferenceIndex) => {
      if (!result.delivered) return sum;
      const matchedFeed = feeds.find((feed) => feed.sequence === result.matchedFeedSequence);
      const reference = references[references.length - 1 - reversedReferenceIndex];
      return sum + Math.round(Math.abs(matchedFeed.receivedMonoMs - reference.receivedMonoMs));
    }, 0);
    assert.deepEqual(
      { matched: reversed.matched, cost: reversedCost },
      expected,
      `permutation iteration ${iteration}`,
    );
  }
});

// Intent: a burst of equal-value trades must conserve one-to-one cardinality
// and choose the globally nearest copies instead of collapsing duplicates.
test("matcher handles two hundred identical trades without reuse or loss", () => {
  const count = 200;
  const references = Array.from({ length: count }, (_, index) =>
    krakenTrade({ tradeId: String(index), receivedMonoMs: index * 10 }));
  const feeds = Array.from({ length: count }, (_, index) =>
    feedTrade({ sequence: index + 1, receivedMonoMs: (count - index - 1) * 10 + 3 }));
  const metrics = computeTradeMetrics(references, feeds, { maxLeadMs: 3_000, maxLagMs: 3_000 });
  const sequences = metrics.allTrades.map((trade) => trade.matchedFeedSequence);
  assert.equal(metrics.matched, count);
  assert.equal(new Set(sequences).size, count);
  assert.equal(
    metrics.allTrades.reduce((sum, trade) => sum + Math.abs(trade.signedDelayMs), 0),
    count * 3,
  );
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

// Intent: when several faults coexist, verdict authority and threshold
// boundaries must remain stable instead of changing with incidental fields.
test("verdict precedence and the slow threshold are exhaustive", () => {
  const healthySession = {
    feed: { connectFailed: false, closes: 0, errors: 0 },
    kraken: { connectFailed: false, disconnected: false },
  };
  const healthyMetrics = {
    referenceTrades: 2,
    feedCandidates: [feedTrade()],
    matched: 2,
    feedMessages: 1,
    feedParseFailures: 0,
    krakenParseFailures: 0,
    delaySlowMs: 5_000,
  };
  const cases = [
    [
      { ...healthySession, feed: { ...healthySession.feed, connectFailed: true }, kraken: { connectFailed: true, disconnected: true } },
      { ...healthyMetrics, krakenParseFailures: 1 },
      ["down", "connect_failed"],
    ],
    [{ ...healthySession, kraken: { ...healthySession.kraken, disconnected: true } }, healthyMetrics, ["inconclusive", "kraken_disconnected"]],
    [{ ...healthySession, kraken: { ...healthySession.kraken, connectFailed: true } }, healthyMetrics, ["inconclusive", "kraken_unavailable"]],
    [healthySession, { ...healthyMetrics, krakenParseFailures: 1 }, ["inconclusive", "kraken_parse_failure"]],
    [healthySession, { ...healthyMetrics, referenceTrades: 0 }, ["inconclusive", "quiet_market"]],
    [healthySession, { ...healthyMetrics, feedCandidates: [], matched: 0, feedMessages: 0 }, ["down", "feed_silent"]],
    [healthySession, { ...healthyMetrics, feedCandidates: [], matched: 0, feedMessages: 1 }, ["down", "invalid_feed_messages"]],
    [healthySession, { ...healthyMetrics, matched: 0 }, ["down", "no_matches"]],
    [{ ...healthySession, feed: { ...healthySession.feed, closes: 1 } }, { ...healthyMetrics, feedParseFailures: 1, matched: 1 }, ["degraded", "socket_dropped"]],
    [{ ...healthySession, feed: { ...healthySession.feed, errors: 1 } }, healthyMetrics, ["degraded", "socket_dropped"]],
    [healthySession, { ...healthyMetrics, feedParseFailures: 1, matched: 1 }, ["degraded", "invalid_feed_messages"]],
    [healthySession, { ...healthyMetrics, matched: 1, delaySlowMs: 9_000 }, ["degraded", "missing_trades"]],
    [healthySession, { ...healthyMetrics, delaySlowMs: 5_001 }, ["degraded", "slow_delivery"]],
    [healthySession, healthyMetrics, ["ok", ""]],
  ];
  for (const [session, metrics, expected] of cases) {
    const verdict = judgeProbe(session, metrics, 5_000);
    assert.deepEqual([verdict.verdict, verdict.note], expected);
  }
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

function bruteForceBoundedMatching(references, feeds, limits) {
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
      const reference = references[referenceIndex];
      const feed = feeds[feedIndex];
      if (tradeMatchKey(reference) !== tradeMatchKey(feed)) continue;
      const delay = feed.receivedMonoMs - reference.receivedMonoMs;
      if (delay < -limits.maxLeadMs || delay > limits.maxLagMs) continue;
      if (Math.abs(feed.exchangeAtMs - reference.exchangeAtMs) > limits.maxExchangeSkewMs) continue;
      usedFeed.add(feedIndex);
      visit(referenceIndex + 1, usedFeed, matched + 1, cost + Math.round(Math.abs(delay)));
      usedFeed.delete(feedIndex);
    }
  };
  visit(0, new Set(), 0, 0);
  return best;
}

const DEFAULT_BURST_GAP_MS = 2_000;

export function parseFeedTrade(raw, receivedAtMs, receivedMonoMs) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return { trade: null, parseFailures: 1 };
  }

  const exchangeAtMs = parseExchangeTimeMs(payload?.time);
  const price = Number(payload?.price);
  const quantity = Number(payload?.quantity);
  const side = normalizeSide(payload?.type);
  if (
    exchangeAtMs === null ||
    !Number.isFinite(price) ||
    !Number.isFinite(quantity) ||
    side === null
  ) {
    return { trade: null, parseFailures: 1 };
  }

  return {
    trade: { exchangeAtMs, receivedAtMs, receivedMonoMs, price, quantity, side },
    parseFailures: 0,
  };
}

export function parseKrakenMessage(raw, receivedAtMs, receivedMonoMs) {
  let payload;
  try {
    payload = JSON.parse(raw);
  } catch {
    return { subscription: null, error: null, trades: [], parseFailures: 1 };
  }

  if (payload?.method === "subscribe") {
    return {
      subscription: payload.success === true ? "accepted" : "rejected",
      error: payload.error || null,
      trades: [],
      parseFailures: 0,
    };
  }

  if (payload?.channel !== "trade" || payload?.type !== "update" || !Array.isArray(payload.data)) {
    return { subscription: null, error: null, trades: [], parseFailures: 0 };
  }

  const trades = [];
  let parseFailures = 0;
  for (const rawTrade of payload.data) {
    const exchangeAtMs = parseExchangeTimeMs(rawTrade?.timestamp);
    const price = Number(rawTrade?.price);
    const quantity = Number(rawTrade?.qty);
    const side = normalizeSide(rawTrade?.side);
    if (
      exchangeAtMs === null ||
      !Number.isFinite(price) ||
      !Number.isFinite(quantity) ||
      side === null
    ) {
      parseFailures += 1;
      continue;
    }
    trades.push({
      exchangeAtMs,
      receivedAtMs,
      receivedMonoMs,
      price,
      quantity,
      side,
      tradeId: rawTrade.trade_id == null ? null : String(rawTrade.trade_id),
    });
  }

  return { subscription: null, error: null, trades, parseFailures };
}

export function computeTradeMetrics(reference, feedCandidates, options = {}) {
  const maxLeadMs = options.maxLeadMs ?? 2_000;
  const maxLagMs = options.maxLagMs ?? 10_000;
  const referencesByKey = groupIndices(reference, tradeMatchKey);
  const feedByKey = groupIndices(feedCandidates, tradeMatchKey);
  const matchedByReference = new Map();
  const matchedFeedIndices = new Set();

  for (const [key, referenceIndices] of referencesByKey) {
    const feedIndices = feedByKey.get(key) || [];
    if (feedIndices.length === 0) continue;
    const pairs = minimumCostMaximumPairs(reference, feedCandidates, referenceIndices, feedIndices, {
      maxLeadMs,
      maxLagMs,
    });
    for (const pair of pairs) {
      matchedByReference.set(pair.referenceIndex, pair.feedIndex);
      matchedFeedIndices.add(pair.feedIndex);
    }
  }

  const allTrades = reference.map((krakenTrade, referenceIndex) => {
    const feedIndex = matchedByReference.get(referenceIndex);
    if (feedIndex === undefined) {
      return {
        ...krakenTrade,
        delivered: false,
        delayMs: null,
        signedDelayMs: null,
        feedReceivedAtMs: null,
        matchedFeedSequence: null,
      };
    }

    const feedTrade = feedCandidates[feedIndex];
    const signedDelayMs = Math.round(feedTrade.receivedMonoMs - krakenTrade.receivedMonoMs);
    const delayMs = Math.max(0, signedDelayMs);
    return {
      ...krakenTrade,
      delivered: true,
      delayMs,
      signedDelayMs,
      feedReceivedAtMs: feedTrade.receivedAtMs,
      matchedFeedSequence: feedTrade.sequence ?? null,
    };
  });

  return { ...summarizeTradeResults(allTrades), matchedFeedIndices };
}

export function summarizeTradeResults(allTrades) {
  const delivered = allTrades.filter((trade) => trade.delivered);
  const delays = delivered.map((trade) => trade.delayMs).sort((lhs, rhs) => lhs - rhs);
  const signedDelays = delivered.map((trade) => trade.signedDelayMs).sort((lhs, rhs) => lhs - rhs);
  delays.sort((lhs, rhs) => lhs - rhs);
  return {
    referenceTrades: allTrades.length,
    allTrades,
    matched: delays.length,
    coveragePct: allTrades.length ? Math.round((delays.length / allTrades.length) * 100) : null,
    delayMedianMs: percentileAt(delays, 0.5),
    delaySlowMs: percentileAt(delays, 0.9),
    delayMaxMs: delays.length ? delays[delays.length - 1] : null,
    signedDelayMinMs: signedDelays.length ? signedDelays[0] : null,
    signedDelayMedianMs: percentileAt(signedDelays, 0.5),
  };
}

export function analyzeLossRuns(trades, burstGapMs = DEFAULT_BURST_GAP_MS) {
  const ordered = [...trades].sort(
    (lhs, rhs) => lhs.exchangeAtMs - rhs.exchangeAtMs || lhs.receivedAtMs - rhs.receivedAtMs,
  );
  const runs = [];
  let current = null;

  const closeCurrent = () => {
    if (!current) return;
    const first = current.trades[0];
    const last = current.trades[current.trades.length - 1];
    current.count = current.trades.length;
    current.startedAtMs = first.exchangeAtMs;
    current.endedAtMs = last.exchangeAtMs;
    current.durationMs = Math.max(0, last.exchangeAtMs - first.exchangeAtMs);
    const startsWindow = current.firstIndex === 0;
    const endsWindow = current.lastIndex === ordered.length - 1;
    current.position = startsWindow && endsWindow
      ? "all"
      : startsWindow
        ? "start"
        : endsWindow
          ? "end"
          : "middle";
    delete current.trades;
    runs.push(current);
    current = null;
  };

  for (let index = 0; index < ordered.length; index += 1) {
    const trade = ordered[index];
    if (trade.delivered) {
      closeCurrent();
      continue;
    }

    const previous = current?.trades[current.trades.length - 1];
    if (previous && trade.exchangeAtMs - previous.exchangeAtMs > burstGapMs) {
      closeCurrent();
    }
    if (!current) current = { firstIndex: index, lastIndex: index, trades: [] };
    current.trades.push(trade);
    current.lastIndex = index;
  }
  closeCurrent();

  return {
    totalTrades: ordered.length,
    lostTrades: ordered.filter((trade) => !trade.delivered).length,
    runs,
  };
}

export function judgeProbe(session, metrics, slowDelayMs = 5_000) {
  if (session.feed.connectFailed) return { verdict: "down", note: "connect_failed" };
  if (session.kraken.disconnected) return { verdict: "inconclusive", note: "kraken_disconnected" };
  if (session.kraken.connectFailed) return { verdict: "inconclusive", note: "kraken_unavailable" };
  if (metrics.krakenParseFailures > 0) {
    return { verdict: "inconclusive", note: "kraken_parse_failure" };
  }
  if (metrics.referenceTrades === 0) return { verdict: "inconclusive", note: "quiet_market" };
  if (metrics.feedCandidates.length === 0) {
    return {
      verdict: "down",
      note: metrics.feedMessages > 0 ? "invalid_feed_messages" : "feed_silent",
    };
  }
  if (metrics.matched === 0) return { verdict: "down", note: "no_matches" };
  if (session.feed.closes > 0) return { verdict: "degraded", note: "socket_dropped" };
  if (metrics.feedParseFailures > 0) {
    return { verdict: "degraded", note: "invalid_feed_messages" };
  }
  if (metrics.matched < metrics.referenceTrades) {
    return { verdict: "degraded", note: "missing_trades" };
  }
  if (metrics.delaySlowMs !== null && metrics.delaySlowMs > slowDelayMs) {
    return { verdict: "degraded", note: "slow_delivery" };
  }
  return { verdict: "ok", note: "" };
}

export function tradeMatchKey(trade) {
  // The feed rounds Kraken's sub-second timestamp to the nearest Unix second
  // (for example .949 becomes the next second), rather than flooring it.
  const exchangeSecond = Math.round(trade.exchangeAtMs / 1_000);
  return `${exchangeSecond}|${trade.price}|${trade.quantity}|${trade.side}`;
}

function parseExchangeTimeMs(value) {
  if (value == null) return null;
  if (typeof value === "number" || (typeof value === "string" && /^\d+(?:\.\d+)?$/.test(value))) {
    const number = Number(value);
    if (!Number.isFinite(number)) return null;
    return Math.abs(number) < 100_000_000_000 ? number * 1_000 : number;
  }
  const preciseIso = /^(.*:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})$/.exec(value);
  if (preciseIso?.[2]) {
    const wholeSecondMs = Date.parse(`${preciseIso[1]}${preciseIso[3]}`);
    if (Number.isFinite(wholeSecondMs)) {
      return wholeSecondMs + Number(`0.${preciseIso[2]}`) * 1_000;
    }
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeSide(value) {
  const side = typeof value === "string" ? value.toLowerCase() : "";
  return side === "buy" || side === "sell" ? side : null;
}

function groupIndices(values, keyForValue) {
  const groups = new Map();
  for (let index = 0; index < values.length; index += 1) {
    const key = keyForValue(values[index]);
    const indices = groups.get(key) || [];
    indices.push(index);
    groups.set(key, indices);
  }
  return groups;
}

// Successive shortest augmenting paths give maximum cardinality first and,
// among those matchings, the smallest total receive-time distance. Groups are
// already exact exchange-second + value + side classes, so they remain small.
function minimumCostMaximumPairs(reference, feed, referenceIndices, feedIndices, limits) {
  const source = 0;
  const referenceOffset = 1;
  const feedOffset = referenceOffset + referenceIndices.length;
  const sink = feedOffset + feedIndices.length;
  const graph = Array.from({ length: sink + 1 }, () => []);
  const pairEdges = [];

  const addEdge = (from, to, capacity, cost, pair = null) => {
    const forward = { to, capacity, cost, reverse: graph[to].length, pair };
    const backward = { to: from, capacity: 0, cost: -cost, reverse: graph[from].length, pair: null };
    graph[from].push(forward);
    graph[to].push(backward);
    if (pair) pairEdges.push(forward);
  };

  for (let index = 0; index < referenceIndices.length; index += 1) {
    addEdge(source, referenceOffset + index, 1, 0);
  }
  for (let index = 0; index < feedIndices.length; index += 1) {
    addEdge(feedOffset + index, sink, 1, 0);
  }
  for (let referencePosition = 0; referencePosition < referenceIndices.length; referencePosition += 1) {
    const referenceIndex = referenceIndices[referencePosition];
    for (let feedPosition = 0; feedPosition < feedIndices.length; feedPosition += 1) {
      const feedIndex = feedIndices[feedPosition];
      const signedDelayMs = feed[feedIndex].receivedMonoMs - reference[referenceIndex].receivedMonoMs;
      if (signedDelayMs < -limits.maxLeadMs || signedDelayMs > limits.maxLagMs) continue;
      addEdge(
        referenceOffset + referencePosition,
        feedOffset + feedPosition,
        1,
        Math.round(Math.abs(signedDelayMs)),
        { referenceIndex, feedIndex },
      );
    }
  }

  while (augmentShortestPath(graph, source, sink)) {
    // Every path carries exactly one pair.
  }
  return pairEdges.filter((edge) => edge.capacity === 0).map((edge) => edge.pair);
}

function augmentShortestPath(graph, source, sink) {
  const distance = Array(graph.length).fill(Infinity);
  const previousNode = Array(graph.length).fill(-1);
  const previousEdge = Array(graph.length).fill(-1);
  const queued = Array(graph.length).fill(false);
  const queue = [source];
  distance[source] = 0;
  queued[source] = true;

  for (let cursor = 0; cursor < queue.length; cursor += 1) {
    const node = queue[cursor];
    queued[node] = false;
    for (let edgeIndex = 0; edgeIndex < graph[node].length; edgeIndex += 1) {
      const edge = graph[node][edgeIndex];
      if (edge.capacity <= 0 || distance[edge.to] <= distance[node] + edge.cost) continue;
      distance[edge.to] = distance[node] + edge.cost;
      previousNode[edge.to] = node;
      previousEdge[edge.to] = edgeIndex;
      if (!queued[edge.to]) {
        queued[edge.to] = true;
        queue.push(edge.to);
      }
    }
  }
  if (!Number.isFinite(distance[sink])) return false;

  for (let node = sink; node !== source; node = previousNode[node]) {
    const edge = graph[previousNode[node]][previousEdge[node]];
    edge.capacity -= 1;
    graph[node][edge.reverse].capacity += 1;
  }
  return true;
}

function percentileAt(sorted, percentile) {
  if (sorted.length === 0) return null;
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * percentile))];
}

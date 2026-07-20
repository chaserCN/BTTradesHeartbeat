import assert from "node:assert/strict";
import test from "node:test";

import { collectSession, computeSessionMetrics, previewRawMessage } from "../heartbeat-session.mjs";

const baseTimeMs = Date.parse("2026-07-20T09:05:22.194Z");

// Intent: an event delivered exactly after either boundary timer fires must be
// attributed to the new phase, or sync/drain duplicates can corrupt coverage.
test("lifecycle records warmup, sync, exact t0, measurement, and exact t1 drain phases", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  const feed = harness.socket("feed");
  feed.open();
  harness.advanceTo(1);
  feed.message(feedPayload(1));

  harness.advanceTo(10);
  const kraken = harness.socket("kraken");
  kraken.open();
  kraken.message(subscriptionAccepted());
  feed.message(feedPayload(2));
  kraken.message(krakenPayload(2, "sync"));

  harness.advanceTo(20); // beginMeasurement runs before these messages.
  kraken.message(krakenPayload(3, "t0"));
  feed.message(feedPayload(3));

  harness.advanceTo(49);
  kraken.message(krakenPayload(4, "lost"));

  harness.advanceTo(50); // reference end runs before these messages.
  kraken.message(krakenPayload(5, "drain"));
  feed.message(feedPayload(5));
  kraken.remoteClose(); // A reference close during drain must not invalidate the window.
  harness.advanceTo(55);

  const session = await pending;
  assert.deepEqual(session.feed.events.map((event) => event.phase), [
    "warming_feed", "syncing", "measuring", "draining",
  ]);
  assert.deepEqual(session.kraken.trades.map((trade) => trade.phase), [
    "syncing", "measuring", "measuring", "draining",
  ]);
  assert.equal(session.measurementStartedMonoMs, 20);
  assert.equal(session.referenceEndedMonoMs, 50);
  assert.equal(session.endedAtMonoMs, 55);
  assert.equal(session.windowMs, 30);
  assert.equal(session.kraken.disconnected, false);
  assert.equal(session.feed.subscribedAtMonoMs, 0);
  assert.equal(session.kraken.subscribedAtMonoMs, 10);
  assert.deepEqual(session.feed.socketEvents.map((event) => event.eventType), ["open", "subscribe_sent"]);
  assert.deepEqual(session.kraken.socketEvents.map((event) => event.eventType), [
    "open", "subscribe_sent", "subscribe_accepted", "close",
  ]);

  const metrics = computeSessionMetrics(session, { preRollMs: 10, drainMs: 5 });
  assert.deepEqual(
    {
      sync: [metrics.sync.referenceTrades, metrics.sync.matched],
      measurement: [metrics.referenceTrades, metrics.matched],
      drain: [metrics.drain.referenceTrades, metrics.drain.matched],
    },
    { sync: [1, 1], measurement: [2, 1], drain: [1, 1] },
  );
  assert.deepEqual(metrics.allTrades.map((trade) => trade.tradeId), ["t0", "lost"]);
});

// Intent: failures before a usable overlap window must terminate promptly and
// cannot be reported later as an ordinary feed-loss verdict.
test("feed close during warmup marks connection failure and cancels later phases", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  const feed = harness.socket("feed");
  feed.open();
  harness.advanceTo(5);
  feed.remoteClose({ code: 1006, reason: "upstream reset", wasClean: false });
  const session = await pending;
  assert.equal(session.feed.connectFailed, true);
  assert.equal(session.feed.closes, 1);
  assert.equal(session.feed.subscribedAtMonoMs, 0);
  assert.equal(session.feed.firstMessageAtMonoMs, null);
  assert.deepEqual(session.feed.socketEvents.at(-1), {
    sequence: 3,
    eventType: "close",
    phase: "warming_feed",
    occurredAtMs: baseTimeMs + 5,
    occurredMonoMs: 5,
    code: 1006,
    reason: "upstream reset",
    wasClean: false,
    detail: null,
  });
  assert.equal(session.measurementStartedMonoMs, null);
  harness.advanceTo(100);
  assert.equal(harness.sockets.length, 1, "Kraken must not connect after early finish");
});

test("Kraken close while syncing makes the reference inconclusive", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  harness.socket("feed").open();
  harness.advanceTo(10);
  const kraken = harness.socket("kraken");
  kraken.open();
  kraken.message(subscriptionAccepted());
  harness.advanceTo(15);
  kraken.remoteClose();
  const session = await pending;
  assert.equal(session.kraken.disconnected, true);
  assert.equal(session.measurementStartedMonoMs, null);
});

test("feed error and close during measurement are recorded without erasing the fixed window", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  const feed = harness.socket("feed");
  feed.open();
  harness.advanceTo(10);
  const kraken = harness.socket("kraken");
  kraken.open();
  kraken.message(subscriptionAccepted());
  harness.advanceTo(20);
  feed.error({ message: "temporary network failure" });
  feed.remoteClose({ code: 1001, reason: "peer restart", wasClean: true });
  assert.equal(feed.closed, false, "remote close is distinct from collector cleanup in the fake");
  harness.advanceTo(55);
  const session = await pending;
  assert.equal(session.feed.errors, 1);
  assert.equal(session.feed.closes, 1);
  assert.equal(session.feed.connectFailed, false);
  assert.equal(session.measurementStartedMonoMs, 20);
  assert.equal(session.referenceEndedMonoMs, 50);
  assert.deepEqual(
    session.feed.socketEvents.slice(-2).map((event) => [event.eventType, event.code, event.reason, event.detail]),
    [["error", null, null, "temporary network failure"], ["close", 1001, "peer restart", null]],
  );
});

// Intent: an NTP wall-clock correction during connect must not alter duration
// metrics, while persisted wall and monotonic timestamps retain both views.
test("feed handshake uses monotonic time across a wall-clock jump", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  harness.advanceTo(7);
  harness.jumpWallBy(3_600_000);
  const feed = harness.socket("feed");
  feed.open();
  harness.advanceTo(8);
  feed.remoteClose();
  const session = await pending;
  assert.equal(session.feed.handshakeMs, 7);
  assert.equal(session.feed.subscribedAtMonoMs, 7);
  assert.equal(session.feed.subscribedAtMs, baseTimeMs + 3_600_007);
});

test("Kraken error before subscription acknowledgement is a reference connection failure", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  harness.socket("feed").open();
  harness.advanceTo(10);
  const kraken = harness.socket("kraken");
  kraken.open();
  kraken.error();
  const session = await pending;
  assert.equal(session.kraken.connectFailed, true);
  assert.equal(session.kraken.disconnected, false);
});

test("Kraken error during measurement ends with an incomplete reference window", async () => {
  const harness = createHarness();
  const pending = collectSession(sessionOptions(), harness.dependencies);
  harness.socket("feed").open();
  harness.advanceTo(10);
  const kraken = harness.socket("kraken");
  kraken.open();
  kraken.message(subscriptionAccepted());
  harness.advanceTo(20);
  kraken.error();
  const session = await pending;
  assert.equal(session.kraken.disconnected, true);
  assert.equal(session.kraken.connectFailed, false);
  assert.equal(session.measurementStartedMonoMs, 20);
  assert.equal(session.referenceEndedMonoMs, null);
  assert.equal(session.windowMs, 0);
});

test("feed and Kraken connection timeouts finish with the correct authority failure", async () => {
  const feedHarness = createHarness();
  const feedPending = collectSession(sessionOptions(), feedHarness.dependencies);
  feedHarness.advanceTo(40);
  const feedTimeout = await feedPending;
  assert.equal(feedTimeout.feed.connectFailed, true);
  assert.equal(feedTimeout.kraken.connectFailed, false);

  const krakenHarness = createHarness();
  const krakenPending = collectSession(sessionOptions(), krakenHarness.dependencies);
  krakenHarness.socket("feed").open();
  krakenHarness.advanceTo(10);
  krakenHarness.socket("kraken").open(); // No subscription acknowledgement.
  krakenHarness.advanceTo(50);
  const krakenTimeout = await krakenPending;
  assert.equal(krakenTimeout.feed.connectFailed, false);
  assert.equal(krakenTimeout.kraken.connectFailed, true);
});

// Intent: callbacks already queued when a timeout closes a socket must not
// mutate the resolved session or send a subscription after finalization.
test("late socket callbacks after timeout are inert", async () => {
  const feedHarness = createHarness();
  const feedPending = collectSession(sessionOptions(), feedHarness.dependencies);
  const feed = feedHarness.socket("feed");
  feedHarness.advanceTo(40);
  const feedSession = await feedPending;
  feed.open();
  feed.error();
  feed.message(feedPayload(9));
  feedHarness.advanceTo(100);
  assert.equal(feedSession.feed.handshakeMs, null);
  assert.equal(feedSession.feed.errors, 0);
  assert.equal(feedSession.feed.messages, 0);
  assert.deepEqual(feed.sent, []);
  assert.equal(feedHarness.sockets.length, 1);

  const krakenHarness = createHarness();
  const krakenPending = collectSession(sessionOptions(), krakenHarness.dependencies);
  krakenHarness.socket("feed").open();
  krakenHarness.advanceTo(10);
  const kraken = krakenHarness.socket("kraken");
  krakenHarness.advanceTo(50);
  const krakenSession = await krakenPending;
  kraken.open();
  kraken.message(subscriptionAccepted());
  assert.equal(krakenSession.kraken.connected, false);
  assert.equal(krakenSession.kraken.messages, 0);
  assert.deepEqual(kraken.sent, []);
});

test("subscription rejection and constructor failures terminate in the owning stream", async () => {
  const rejectedHarness = createHarness();
  const rejectedPending = collectSession(sessionOptions(), rejectedHarness.dependencies);
  rejectedHarness.socket("feed").open();
  rejectedHarness.advanceTo(10);
  const rejectedKraken = rejectedHarness.socket("kraken");
  rejectedKraken.open();
  rejectedKraken.message(JSON.stringify({ method: "subscribe", success: false, error: "bad pair" }));
  assert.equal((await rejectedPending).kraken.connectFailed, true);

  const feedThrowHarness = createHarness({ throwUrl: "ws://feed" });
  const feedFailure = await collectSession(sessionOptions(), feedThrowHarness.dependencies);
  assert.equal(feedFailure.feed.connectFailed, true);

  const krakenThrowHarness = createHarness({ throwUrl: "ws://kraken" });
  const krakenPending = collectSession(sessionOptions(), krakenThrowHarness.dependencies);
  krakenThrowHarness.socket("feed").open();
  krakenThrowHarness.advanceTo(10);
  const krakenFailure = await krakenPending;
  assert.equal(krakenFailure.kraken.connectFailed, true);

  const feedSendHarness = createHarness({ throwSendUrl: "ws://feed" });
  const feedSendPending = collectSession(sessionOptions(), feedSendHarness.dependencies);
  feedSendHarness.socket("feed").open();
  const feedSendFailure = await feedSendPending;
  assert.equal(feedSendFailure.feed.connectFailed, true);
  assert.equal(feedSendFailure.feed.socketEvents.at(-1).eventType, "subscribe_send_error");

  const krakenSendHarness = createHarness({ throwSendUrl: "ws://kraken" });
  const krakenSendPending = collectSession(sessionOptions(), krakenSendHarness.dependencies);
  krakenSendHarness.socket("feed").open();
  krakenSendHarness.advanceTo(10);
  krakenSendHarness.socket("kraken").open();
  const krakenSendFailure = await krakenSendPending;
  assert.equal(krakenSendFailure.kraken.connectFailed, true);
  assert.equal(krakenSendFailure.kraken.socketEvents.at(-1).eventType, "subscribe_send_error");
});

// Intent: only feed failures in the delivery horizon and Kraken failures in
// the reference window may affect verdict telemetry at exact boundaries.
test("parse-failure authority respects sync, measurement, and drain boundaries", () => {
  const makeTrade = (receivedMonoMs, marker, source) => ({
    exchangeAtMs: baseTimeMs + marker,
    receivedAtMs: baseTimeMs + receivedMonoMs,
    receivedMonoMs,
    price: 60_000 + marker,
    quantity: marker / 100_000_000,
    side: "buy",
    ...(source === "kraken" ? { tradeId: String(marker) } : { sequence: marker }),
  });
  const session = {
    measurementStartedMonoMs: 100,
    referenceEndedMonoMs: 200,
    endedAtMonoMs: 210,
    syncStartedMonoMs: 50,
    feed: {
      trades: [makeTrade(91, 1, "feed"), makeTrade(101, 2, "feed"), makeTrade(201, 3, "feed")],
      events: [
        { receivedMonoMs: 99, parseFailures: 8 },
        { receivedMonoMs: 100, parseFailures: 1 },
        { receivedMonoMs: 200, parseFailures: 2 },
        { receivedMonoMs: 210, parseFailures: 4 },
        { receivedMonoMs: 211, parseFailures: 16 },
      ],
    },
    kraken: {
      trades: [makeTrade(90, 1, "kraken"), makeTrade(100, 2, "kraken"), makeTrade(200, 3, "kraken")],
      events: [
        { receivedMonoMs: 99, parseFailures: 8 },
        { receivedMonoMs: 100, parseFailures: 1 },
        { receivedMonoMs: 199, parseFailures: 2 },
        { receivedMonoMs: 200, parseFailures: 4 },
      ],
    },
  };
  const metrics = computeSessionMetrics(session, { preRollMs: 10, drainMs: 10 });
  assert.deepEqual(
    {
      sync: [metrics.sync.referenceTrades, metrics.sync.matched],
      measurement: [metrics.referenceTrades, metrics.matched],
      drain: [metrics.drain.referenceTrades, metrics.drain.matched],
      deliveryHorizonFeedParseFailures: metrics.deliveryHorizonFeedParseFailures,
      referenceWindowKrakenParseFailures: metrics.referenceWindowKrakenParseFailures,
    },
    {
      sync: [1, 1],
      measurement: [1, 1],
      drain: [1, 1],
      deliveryHorizonFeedParseFailures: 7,
      referenceWindowKrakenParseFailures: 3,
    },
  );
});

test("failed pre-measurement sessions produce empty neutral metrics", () => {
  const metrics = computeSessionMetrics({
    measurementStartedMonoMs: null,
    feed: { trades: [], events: [] },
    kraken: { trades: [], events: [] },
  }, { preRollMs: 2_000, drainMs: 10_000 });
  assert.deepEqual(
    {
      references: metrics.referenceTrades,
      matched: metrics.matched,
      coverage: metrics.coveragePct,
      deliveryHorizonFeedMessages: metrics.deliveryHorizonFeedMessages,
      deliveryHorizonFeedParseFailures: metrics.deliveryHorizonFeedParseFailures,
      referenceWindowKrakenParseFailures: metrics.referenceWindowKrakenParseFailures,
    },
    {
      references: 0,
      matched: 0,
      coverage: null,
      deliveryHorizonFeedMessages: 0,
      deliveryHorizonFeedParseFailures: 0,
      referenceWindowKrakenParseFailures: 0,
    },
  );
});

test("raw diagnostic previews support string, ArrayBuffer, and typed-array frames", () => {
  assert.equal(previewRawMessage("x".repeat(600)).length, 500);
  assert.equal(previewRawMessage(new TextEncoder().encode("array buffer").buffer), "array buffer");
  const bytes = new TextEncoder().encode("typed array");
  assert.equal(previewRawMessage(bytes.subarray(0)), "typed array");
});

function sessionOptions() {
  return {
    windowMs: 30,
    warmupMs: 10,
    drainMs: 5,
    connectTimeoutMs: 40,
    feedUrl: "ws://feed",
    feedChannel: "trade.test",
    krakenUrl: "ws://kraken",
    krakenSymbol: "BTC/USD",
  };
}

function createHarness(options = {}) {
  const scheduler = new ManualScheduler();
  const sockets = [];
  let wallOffsetMs = 0;
  class FakeWebSocket {
    constructor(url) {
      if (url === options.throwUrl) throw new Error("constructor failure");
      this.url = url;
      this.sent = [];
      this.closed = false;
      sockets.push(this);
    }

    send(payload) {
      if (this.url === options.throwSendUrl) throw new Error("send failure");
      this.sent.push(payload);
    }
    close() { this.closed = true; }
    open() { this.onopen?.(); }
    message(data) { this.onmessage?.({ data }); }
    error(event = {}) { this.onerror?.(event); }
    remoteClose(event = {}) { this.onclose?.(event); }
  }
  return {
    sockets,
    dependencies: {
      WebSocketClass: FakeWebSocket,
      wallNow: () => baseTimeMs + scheduler.now + wallOffsetMs,
      monoNow: () => scheduler.now,
      setTimer: (callback, delay) => scheduler.setTimer(callback, delay),
      clearTimer: (id) => scheduler.clearTimer(id),
      logError: () => {},
    },
    advanceTo: (time) => scheduler.advanceTo(time),
    jumpWallBy(deltaMs) { wallOffsetMs += deltaMs; },
    socket(name) {
      const index = name === "feed" ? 0 : 1;
      assert.ok(sockets[index], `${name} socket has not been created`);
      return sockets[index];
    },
  };
}

class ManualScheduler {
  now = 0;
  #nextId = 1;
  #timers = new Map();

  setTimer(callback, delay) {
    const id = this.#nextId++;
    this.#timers.set(id, { id, at: this.now + delay, callback });
    return id;
  }

  clearTimer(id) {
    this.#timers.delete(id);
  }

  advanceTo(target) {
    assert.ok(target >= this.now);
    while (true) {
      const due = [...this.#timers.values()]
        .filter((timer) => timer.at <= target)
        .sort((lhs, rhs) => lhs.at - rhs.at || lhs.id - rhs.id)[0];
      if (!due) break;
      this.#timers.delete(due.id);
      this.now = due.at;
      due.callback();
    }
    this.now = target;
  }
}

function feedPayload(marker) {
  return JSON.stringify({
    time: (baseTimeMs + marker) / 1_000,
    price: 60_000 + marker,
    quantity: marker / 100_000_000,
    type: "buy",
  });
}

function krakenPayload(marker, tradeId) {
  return JSON.stringify({
    channel: "trade",
    type: "update",
    data: [{
      timestamp: new Date(baseTimeMs + marker).toISOString(),
      price: 60_000 + marker,
      qty: marker / 100_000_000,
      side: "buy",
      trade_id: tradeId,
    }],
  });
}

function subscriptionAccepted() {
  return JSON.stringify({ method: "subscribe", success: true });
}

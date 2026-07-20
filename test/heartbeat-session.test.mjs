import assert from "node:assert/strict";
import test from "node:test";

import { collectSession, computeSessionMetrics } from "../heartbeat-session.mjs";

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
  feed.remoteClose();
  const session = await pending;
  assert.equal(session.feed.connectFailed, true);
  assert.equal(session.feed.closes, 1);
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
  feed.error();
  feed.remoteClose();
  assert.equal(feed.closed, false, "remote close is distinct from collector cleanup in the fake");
  harness.advanceTo(55);
  const session = await pending;
  assert.equal(session.feed.errors, 1);
  assert.equal(session.feed.closes, 1);
  assert.equal(session.feed.connectFailed, false);
  assert.equal(session.measurementStartedMonoMs, 20);
  assert.equal(session.referenceEndedMonoMs, 50);
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

function createHarness() {
  const scheduler = new ManualScheduler();
  const sockets = [];
  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.sent = [];
      this.closed = false;
      sockets.push(this);
    }

    send(payload) { this.sent.push(payload); }
    close() { this.closed = true; }
    open() { this.onopen?.(); }
    message(data) { this.onmessage?.({ data }); }
    error() { this.onerror?.(); }
    remoteClose() { this.onclose?.(); }
  }
  return {
    sockets,
    dependencies: {
      WebSocketClass: FakeWebSocket,
      wallNow: () => baseTimeMs + scheduler.now,
      monoNow: () => scheduler.now,
      setTimer: (callback, delay) => scheduler.setTimer(callback, delay),
      clearTimer: (id) => scheduler.clearTimer(id),
      logError: () => {},
    },
    advanceTo: (time) => scheduler.advanceTo(time),
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

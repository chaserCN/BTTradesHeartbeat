import assert from "node:assert/strict";
import test from "node:test";

import {
  clearQuietPeriod,
  collectQuietHoursDigest,
  QUIET_PERIOD_START_KEY,
  rememberQuietPeriodStart,
  runScheduledProbeSequence,
} from "../heartbeat-cycle.mjs";

// Intent: every reference failure, including one during problem confirmation,
// gets exactly one retry while the sequence remains finite and fully recorded.
test("confirmation retries a failed Kraken reference before choosing the final verdict", async () => {
  const degraded = probe("degraded", "missing_trades", 1);
  const krakenFailure = probe("inconclusive", "kraken_disconnected", 2);
  const recovered = probe("ok", "", 3);
  const result = await runSequence([degraded, krakenFailure, recovered], "ok");
  assert.equal(result.final, recovered);
  assert.deepEqual(result.recorded, [1, 2, 3]);
  assert.deepEqual(result.waits, [120_000, 120_000]);
  assert.equal(result.calls, 3);
});

test("two consecutive Kraken failures stop after one reference retry", async () => {
  const first = probe("inconclusive", "kraken_unavailable", 1);
  const second = probe("inconclusive", "kraken_parse_failure", 2);
  const result = await runSequence([first, second], "ok");
  assert.equal(result.final, second);
  assert.deepEqual(result.recorded, [1, 2]);
  assert.deepEqual(result.waits, [120_000]);
  assert.equal(result.calls, 2);
});

test("composed reference and confirmation retries have a hard four-probe ceiling", async () => {
  const probes = [
    probe("inconclusive", "kraken_unavailable", 1),
    probe("degraded", "missing_trades", 2),
    probe("inconclusive", "kraken_disconnected", 3),
    probe("down", "no_matches", 4),
  ];
  const result = await runSequence(probes, "ok");
  assert.equal(result.final, probes[3]);
  assert.deepEqual(result.recorded, [1, 2, 3, 4]);
  assert.deepEqual(result.waits, [120_000, 120_000, 120_000]);
  assert.equal(result.calls, 4);
});

test("a new problem is confirmed once, but an already notified problem is not", async () => {
  const firstProblem = probe("degraded", "missing_trades", 1);
  const confirmed = probe("down", "no_matches", 2);
  const changed = await runSequence([firstProblem, confirmed], "ok");
  assert.equal(changed.final, confirmed);
  assert.deepEqual(changed.recorded, [1, 2]);
  assert.deepEqual(changed.waits, [120_000]);

  const repeated = await runSequence([firstProblem], "degraded");
  assert.equal(repeated.final, firstProblem);
  assert.deepEqual(repeated.recorded, [1]);
  assert.deepEqual(repeated.waits, []);
});

test("healthy result records once without sleeping", async () => {
  const healthy = probe("ok", "", 1);
  const result = await runSequence([healthy], "ok");
  assert.equal(result.final, healthy);
  assert.deepEqual(result.recorded, [1]);
  assert.deepEqual(result.waits, []);
});

// --- quiet hours catch-up ---------------------------------------------------

// Intent: a problem that both starts and ends while notifications are muted
// must still reach the chat, because by morning the live verdict is `ok` again
// and the existing catch-up has nothing left to report.
test("problems muted overnight are summarized for the morning", () => {
  const kv = fakeKv({ [QUIET_PERIOD_START_KEY]: String(hoursUtc(23)) });
  const night = [
    nightProbe(1, 23.7, "ok", ""),
    nightProbe(2, 25.7, "degraded", "missing_trades"),
    nightProbe(3, 26.7, "down", "feed_silent"),
    nightProbe(4, 27.7, "inconclusive", "kraken_unavailable"),
    nightProbe(5, 28.7, "ok", ""),
    nightProbe(6, 33.7, "ok", ""), // the morning probe itself, already recorded
  ];

  const digest = collectQuietHoursDigest({
    ...kv,
    getProbesSince: () => night,
    isQuietHours,
    nowMs: hoursUtc(33.7),
  });

  assert.equal(digest.total, 5, "the morning probe is not part of the night");
  assert.deepEqual(digest.problems.map((entry) => entry.id), [2, 3]);
  assert.deepEqual(digest.referenceFailures.map((entry) => entry.id), [4]);
  assert.equal(digest.worstVerdict, "down");
  // Still pending: a failed Telegram send must not silently drop the night.
  assert.equal(kv.kvGet(QUIET_PERIOD_START_KEY), String(hoursUtc(23)));

  clearQuietPeriod(kv);
  assert.equal(collectQuietHoursDigest({
    ...kv,
    getProbesSince: () => night,
    isQuietHours,
    nowMs: hoursUtc(33.7),
  }), null, "a delivered digest is never repeated");
});

// Intent: silence must stay silent — a healthy or merely unassessable night
// produces no morning message, and leaves no marker behind to fire later.
test("a clean quiet period sends nothing and releases the marker", () => {
  const kv = fakeKv({ [QUIET_PERIOD_START_KEY]: String(hoursUtc(23)) });
  const digest = collectQuietHoursDigest({
    ...kv,
    getProbesSince: () => [
      nightProbe(1, 23.7, "ok", ""),
      nightProbe(2, 25.7, "inconclusive", "quiet_market"),
    ],
    isQuietHours,
    nowMs: hoursUtc(33.7),
  });
  assert.equal(digest, null);
  assert.equal(kv.kvGet(QUIET_PERIOD_START_KEY), "");
});

// Intent: the marker is persistent state, so a stale or damaged one must not
// turn into an unbounded history query or an exception on the next morning.
test("the digest window is bounded and a damaged marker is discarded", () => {
  const requested = [];
  const kv = fakeKv({ [QUIET_PERIOD_START_KEY]: String(hoursUtc(-500)) });
  collectQuietHoursDigest({
    ...kv,
    getProbesSince: (sinceMs) => {
      requested.push(sinceMs);
      return [];
    },
    isQuietHours,
    nowMs: hoursUtc(33.7),
  });
  assert.deepEqual(requested, [hoursUtc(33.7) - 24 * 3_600_000]);

  const damaged = fakeKv({ [QUIET_PERIOD_START_KEY]: "not-a-timestamp" });
  assert.equal(collectQuietHoursDigest({
    ...damaged,
    getProbesSince: () => assert.fail("a damaged marker must not reach the history"),
    isQuietHours,
    nowMs: hoursUtc(33.7),
  }), null);
  assert.equal(damaged.kvGet(QUIET_PERIOD_START_KEY), "");

  const idle = fakeKv({});
  assert.equal(collectQuietHoursDigest({
    ...idle,
    getProbesSince: () => assert.fail("no quiet period, no history query"),
    isQuietHours,
    nowMs: hoursUtc(33.7),
  }), null);
  assert.equal(idle.writes, 0, "a day without quiet hours writes nothing");
});

// Intent: the digest must cover the whole night, so later muted probes cannot
// move its start forward.
test("the quiet period keeps the timestamp of its first muted probe", () => {
  const kv = fakeKv({});
  rememberQuietPeriodStart(kv, hoursUtc(23));
  rememberQuietPeriodStart(kv, hoursUtc(24));
  rememberQuietPeriodStart(kv, hoursUtc(25));
  assert.equal(kv.kvGet(QUIET_PERIOD_START_KEY), String(hoursUtc(23)));
  assert.equal(kv.writes, 1);
});

function fakeKv(initial) {
  const values = new Map(Object.entries(initial));
  const kv = {
    kvGet: (key) => values.get(key),
    kvSet: (key, value) => {
      kv.writes += 1;
      values.set(key, String(value));
    },
    writes: 0,
  };
  return kv;
}

// Hours from 2026-07-20T00:00Z; the fake quiet window is 23:00-09:00 UTC, so
// hour 23 is the first muted probe and hour 33 (09:00 next day) is the morning.
function hoursUtc(hours) {
  return Date.parse("2026-07-20T00:00:00Z") + hours * 3_600_000;
}

function isQuietHours(date) {
  const hour = date.getUTCHours() + date.getUTCMinutes() / 60;
  return hour >= 23 || hour < 9;
}

function nightProbe(id, hours, verdict, note) {
  return { id, at: new Date(hoursUtc(hours)).toISOString(), verdict, note };
}

function probe(verdict, note, id) {
  return { verdict, note, id };
}

async function runSequence(probes, lastNotified) {
  let calls = 0;
  const recorded = [];
  const waits = [];
  const final = await runScheduledProbeSequence({
    runProbe: async () => {
      assert.ok(calls < probes.length, "sequence requested an unbounded extra probe");
      return probes[calls++];
    },
    recordProbe: (value) => recorded.push(value.id),
    wait: async (milliseconds) => waits.push(milliseconds),
    confirmDelayMs: 120_000,
    getLastNotifiedVerdict: () => lastNotified,
  });
  return { final, recorded, waits, calls };
}

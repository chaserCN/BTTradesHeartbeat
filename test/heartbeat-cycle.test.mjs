import assert from "node:assert/strict";
import test from "node:test";

import { runScheduledProbeSequence } from "../heartbeat-cycle.mjs";

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

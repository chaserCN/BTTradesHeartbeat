import assert from "node:assert/strict";
import test from "node:test";

import {
  agreeUkCount,
  CHECK_NOMINATIVE_FORMS,
  formatUkCount,
  MESSAGE_ACCUSATIVE_FORMS,
  selectUkCountForm,
  TRADE_ACCUSATIVE_FORMS,
  TRADE_NOMINATIVE_FORMS,
} from "../ukrainian.mjs";

// Intent: every numeric suffix, including compound numbers and the 11–14
// exception, must select the Ukrainian one/few/many form deterministically.
test("Ukrainian count categories cover decimal endings and teen exceptions", () => {
  const cases = [
    [0, "угод"],
    [1, "угода"],
    [2, "угоди"],
    [4, "угоди"],
    [5, "угод"],
    [10, "угод"],
    [11, "угод"],
    [12, "угод"],
    [14, "угод"],
    [20, "угод"],
    [21, "угода"],
    [22, "угоди"],
    [24, "угоди"],
    [25, "угод"],
    [101, "угода"],
    [102, "угоди"],
    [111, "угод"],
    [112, "угод"],
    [114, "угод"],
  ];
  for (const [count, expected] of cases) {
    assert.equal(selectUkCountForm(count, TRADE_NOMINATIVE_FORMS), expected, String(count));
  }
});

test("trade, check, and message nouns retain their grammatical case", () => {
  assert.equal(formatUkCount(1, TRADE_NOMINATIVE_FORMS), "1 угода");
  assert.equal(formatUkCount(1, TRADE_ACCUSATIVE_FORMS), "1 угоду");
  assert.equal(formatUkCount(2, TRADE_ACCUSATIVE_FORMS), "2 угоди");
  assert.equal(formatUkCount(5, TRADE_ACCUSATIVE_FORMS), "5 угод");

  assert.equal(formatUkCount(1, CHECK_NOMINATIVE_FORMS), "1 перевірка");
  assert.equal(formatUkCount(4, CHECK_NOMINATIVE_FORMS), "4 перевірки");
  assert.equal(formatUkCount(11, CHECK_NOMINATIVE_FORMS), "11 перевірок");

  assert.equal(formatUkCount(1, MESSAGE_ACCUSATIVE_FORMS), "1 повідомлення");
  assert.equal(formatUkCount(3, MESSAGE_ACCUSATIVE_FORMS), "3 повідомлення");
  assert.equal(formatUkCount(12, MESSAGE_ACCUSATIVE_FORMS), "12 повідомлень");
});

test("verbs agree with singular compound counts", () => {
  for (const count of [1, 21, 101]) {
    assert.equal(agreeUkCount(count, "припала", "припали"), "припала", String(count));
  }
  for (const count of [0, 2, 5, 11, 22, 112]) {
    assert.equal(agreeUkCount(count, "припала", "припали"), "припали", String(count));
  }
});

test("invalid count contracts fail loudly instead of choosing a plausible suffix", () => {
  assert.throws(() => selectUkCountForm(1.5, TRADE_NOMINATIVE_FORMS), /integer/);
  assert.throws(() => selectUkCountForm(Number.NaN, TRADE_NOMINATIVE_FORMS), /integer/);
  assert.throws(() => selectUkCountForm(1, ["one", "many"]), /forms/);
});

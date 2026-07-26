export async function runScheduledProbeSequence(options) {
  const {
    runProbe,
    recordProbe,
    wait,
    confirmDelayMs,
    getLastNotifiedVerdict,
  } = options;
  const log = options.log ?? (() => {});

  const runWithReferenceRetry = async () => {
    let probe = await runProbe();
    recordProbe(probe);
    if (isKrakenReferenceFailure(probe)) {
      log(
        `Kraken reference failed (${probe.note}). ` +
          `Retrying in ${Math.round(confirmDelayMs / 1000)}s.`,
      );
      await wait(confirmDelayMs);
      probe = await runProbe();
      recordProbe(probe);
    }
    return probe;
  };

  let probe = await runWithReferenceRetry();
  const lastNotified = getLastNotifiedVerdict() || "ok";
  if (isProblemVerdict(probe) && probe.verdict !== lastNotified) {
    log(
      `Verdict "${probe.verdict}" differs from last notified "${lastNotified}". ` +
        `Confirming in ${Math.round(confirmDelayMs / 1000)}s.`,
    );
    await wait(confirmDelayMs);
    probe = await runWithReferenceRetry();
  }
  return probe;
}

export function isKrakenReferenceFailure(probe) {
  return probe.note === "kraken_unavailable" ||
    probe.note === "kraken_disconnected" ||
    probe.note === "kraken_parse_failure";
}

export function isProblemVerdict(probe) {
  return probe.verdict === "down" || probe.verdict === "degraded";
}

// --- quiet hours catch-up ---------------------------------------------------
// Quiet hours only mute notifications, so a problem that came and went
// overnight used to leave no trace in the chat at all: by morning the verdict
// is `ok` again and there is nothing left to catch up on. The muted period is
// therefore remembered in `kv` (a restart must not lose it) and summarized once
// from the recorded probes.

export const QUIET_PERIOD_START_KEY = "quietPeriodStartedAtMs";

const DEFAULT_QUIET_DIGEST_LOOKBACK_MS = 24 * 3_600_000;

export function rememberQuietPeriodStart({ kvGet, kvSet }, startedAtMs) {
  if (kvGet(QUIET_PERIOD_START_KEY)) return;
  kvSet(QUIET_PERIOD_START_KEY, startedAtMs);
}

export function clearQuietPeriod({ kvSet }) {
  kvSet(QUIET_PERIOD_START_KEY, "");
}

// Called on the first probe after quiet hours end. Returns null when the night
// was clean — and only then clears the marker itself: a real digest stays
// pending until the caller confirms Telegram accepted it, so a failed send is
// retried on the next probe instead of being swallowed.
export function collectQuietHoursDigest(deps) {
  const {
    kvGet,
    kvSet,
    getProbesSince,
    isQuietHours,
    nowMs,
    maxLookbackMs = DEFAULT_QUIET_DIGEST_LOOKBACK_MS,
  } = deps;

  const marker = kvGet(QUIET_PERIOD_START_KEY);
  if (!marker) return null;

  const startedAtMs = Number(marker);
  if (!Number.isFinite(startedAtMs) || startedAtMs <= 0) {
    clearQuietPeriod({ kvSet });
    return null;
  }

  // A marker left behind by a bot that was down over several mornings must not
  // turn into an unbounded query; one night always fits in the lookback.
  const entries = getProbesSince(Math.max(startedAtMs, nowMs - maxLookbackMs))
    .filter((entry) => isQuietHours(new Date(entry.at)));

  const digest = summarizeQuietHoursProblems(entries);
  if (!digest) clearQuietPeriod({ kvSet });
  return digest;
}

// Only results that would have reached the chat live are worth a morning
// message, so a quiet market or a healthy night stays silent in the morning too.
export function summarizeQuietHoursProblems(entries) {
  const problems = entries.filter(isProblemVerdict);
  const referenceFailures = entries.filter(isKrakenReferenceFailure);
  if (problems.length === 0 && referenceFailures.length === 0) return null;

  return {
    total: entries.length,
    problems,
    referenceFailures,
    worstVerdict: problems.some((entry) => entry.verdict === "down")
      ? "down"
      : problems.length > 0
        ? "degraded"
        : "inconclusive",
  };
}

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

function isProblemVerdict(probe) {
  return probe.verdict === "down" || probe.verdict === "degraded";
}

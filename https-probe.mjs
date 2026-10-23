export const DEFAULT_CHART_URL = "https://api.bitcoinstat.org/api_v3/chart/?coin=btc&currency=usd&exchange=kraken&term=24h&short=true";

export function httpsConfig(env = process.env) {
  const number = (name, fallback, min, max) => {
    const value = Number(env[name] ?? fallback);
    if (!Number.isFinite(value) || value < min || value > max) throw new Error(`Invalid ${name}`);
    return value;
  };
  return {
    intervalMs: number("HTTPS_INTERVAL_MINUTES", 5, 1, 1440) * 60_000,
    timeoutMs: number("HTTPS_TIMEOUT_SECONDS", 15, 1, 120) * 1000,
    slowMs: number("HTTPS_SLOW_SECONDS", 5, 0.1, 120) * 1000,
    maxAgeMs: number("HTTPS_MAX_AGE_MINUTES", 15, 1, 1440) * 60_000,
  };
}

const numeric = (value) => (typeof value === "number" || (typeof value === "string" && value.trim() !== ""))
  && Number.isFinite(Number(value));

export function validateChart(body, nowMs, maxAgeMs) {
  if (body?.code !== 200) return { verdict: "down", note: "api_error" };
  const chart = body.data?.chart;
  const info = body.data?.info;
  if (!Array.isArray(chart) || chart.length === 0) return { verdict: "down", note: "empty_chart" };
  if (!numeric(info?.last) || Number(info.last) <= 0 || !numeric(info?.timestamp) || Number(info.timestamp) <= 0
    || chart.some((p) => !numeric(p?.t) || Number(p.t) <= 0 || !numeric(p?.p) || Number(p.p) <= 0)) {
    return { verdict: "down", note: "invalid_chart" };
  }
  let latest = 0;
  for (const point of chart) latest = Math.max(latest, Number(point.t));
  const quoteAgeMs = nowMs - Number(info.timestamp) * 1000;
  const chartAgeMs = nowMs - latest * 1000;
  const details = { points: chart.length, quoteAgeMs, chartAgeMs };
  // The current chart bucket may be rounded into the future by a few minutes.
  if (quoteAgeMs < -300_000 || chartAgeMs < -300_000) return { ...details, verdict: "down", note: "future_data" };
  if (quoteAgeMs > maxAgeMs || chartAgeMs > maxAgeMs) return { ...details, verdict: "degraded", note: "stale_data" };
  return { ...details, verdict: "ok", note: "ok" };
}

export async function probeHttps(options = {}) {
  const { timeoutMs = 15_000, slowMs = 5_000, maxAgeMs = 900_000,
    fetchImpl = fetch, now = Date.now, mono = () => performance.now(), maxBytes = 2_000_000 } = options;
  const result = { at: new Date(now()).toISOString(), url: DEFAULT_CHART_URL, status: null, points: 0 };
  const started = mono();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let reader;
  try {
    // Standard TLS verification; redirects (including HTTPS -> HTTP) are failures.
    const response = await fetchImpl(DEFAULT_CHART_URL, {
      headers: { Accept: "application/json", "Cache-Control": "no-cache" },
      redirect: "manual", signal: controller.signal,
    });
    result.status = response.status;
    if (response.status !== 200) {
      await response.body?.cancel();
      return { ...result, verdict: "down", note: "http_error", durationMs: Math.round(mono() - started) };
    }
    reader = response.body.getReader();
    const chunks = [];
    let bytes = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maxBytes) {
        await reader.cancel();
        return { ...result, verdict: "down", note: "oversize", durationMs: Math.round(mono() - started) };
      }
      chunks.push(Buffer.from(value));
    }
    const durationMs = Math.round(mono() - started);
    let body;
    try { body = JSON.parse(Buffer.concat(chunks).toString("utf8")); }
    catch { return { ...result, durationMs, verdict: "down", note: "invalid_json" }; }
    const validation = validateChart(body, now(), maxAgeMs);
    if (validation.verdict === "ok" && durationMs > slowMs) Object.assign(validation, { verdict: "degraded", note: "slow" });
    return { ...result, durationMs, ...validation };
  } catch {
    return { ...result, durationMs: Math.round(mono() - started), verdict: "down", note: controller.signal.aborted ? "timeout" : "network_error" };
  } finally {
    clearTimeout(timer);
    reader?.releaseLock();
  }
}

export function describeHttps(probe) {
  if (!probe) return "HTTPS-графік BTC/USD Kraken: перевірок ще не було.";
  const reasons = {
    ok: "працює", http_error: `HTTP ${probe.status}`, api_error: "API повернув помилку",
    empty_chart: "графік порожній", invalid_chart: "некоректні дані графіка",
    future_data: "час даних у майбутньому", stale_data: "дані старіші за допустимий поріг",
    invalid_json: "замість JSON отримано некоректну відповідь", oversize: "відповідь завелика",
    timeout: "сервер не завершив відповідь вчасно", network_error: "помилка мережі, DNS або TLS",
    slow: "графік завантажується повільно",
  };
  const icon = probe.verdict === "ok" ? "🟢" : probe.verdict === "degraded" ? "🟠" : "🔴";
  return `${icon} HTTPS-графік BTC/USD Kraken: ${reasons[probe.note] || "помилка"}. ` +
    `${(probe.durationMs / 1000).toFixed(2)} с; точок: ${probe.points}.`;
}

// Single flight is independent of the long WebSocket probe. Notification state
// is durable and advanced only after Telegram accepts the message.
export function createHttpsMonitor({ store, probe, notify, isQuiet, now = Date.now }) {
  let running = false;
  return {
    get running() { return running; },
    async run() {
      if (running) return null;
      running = true;
      try {
        const result = await probe();
        store.recordHttpsProbe(result);
        const state = JSON.parse(store.kvGet("httpsNotification") || '{"verdict":"ok","quietFailures":0}');
        if (isQuiet()) {
          if (result.verdict !== "ok") {
            state.quietFailures += 1;
            state.lastQuietFailureAt = result.at;
            store.kvSet("httpsNotification", JSON.stringify(state));
          }
          return result;
        }
        if (state.verdict !== result.verdict || state.quietFailures > 0) {
          const digest = state.quietFailures > 0
            ? `\nЗа тихі години: ${state.quietFailures} перевірок із проблемами. Остання: ${state.lastQuietFailureAt}.` : "";
          const recovery = result.verdict === "ok" ? "\nЗараз графік знову доступний." : "";
          await notify(describeHttps(result) + recovery + digest);
          store.kvSet("httpsNotification", JSON.stringify({ verdict: result.verdict, quietFailures: 0, notifiedAt: now() }));
        }
        return result;
      } finally { running = false; }
    },
  };
}

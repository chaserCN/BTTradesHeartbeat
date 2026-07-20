import { analyzeLossRuns } from "./heartbeat-core.mjs";

export const TELEGRAM_MESSAGE_LIMIT = 4_096;

export function formatDetailsMessages(probe, options = {}) {
  const timeZone = options.timeZone ?? "Europe/Kyiv";
  const describeInconclusive = options.describeInconclusive ?? defaultInconclusiveDetail;
  const lines = [
    `${verdictEmoji(probe.verdict)} <b>Перевірка №${probe.id ?? "—"}</b> — ${detailsHeadline(probe)}`,
    `<i>${formatDateTime(probe.at, timeZone)} · вікно ${probe.windowSeconds} с</i>`,
  ];
  if (probe.verdict === "inconclusive") {
    lines.push("", describeInconclusive(probe));
    return [lines.join("\n")];
  }

  lines.push(
    "",
    `Угод на біржі: ${referenceCount(probe)}`,
    `Дійшло: ${probe.matched}${probe.coveragePct !== null ? ` (${probe.coveragePct}%)` : ""}`,
  );
  if (probe.delayMedianMs !== null && probe.delayMedianMs !== undefined) {
    lines.push(`Затримка: зазвичай ${formatDelay(probe.delayMedianMs)}, максимум ${formatDelay(probe.delayMaxMs)}`);
  }
  if (probe.handshakeMs !== null && probe.handshakeMs !== undefined) {
    lines.push(`Підключення: ${formatDelay(probe.handshakeMs)}`);
  }
  if (probe.measurementFeedParseFailures || probe.measurementKrakenParseFailures) {
    lines.push(
      `Помилки розбору у вікні: feed ${probe.measurementFeedParseFailures || 0}, ` +
        `Kraken ${probe.measurementKrakenParseFailures || 0}`,
    );
  }
  if (probe.feedCloses || probe.feedErrors) {
    lines.push(`Обриви з'єднання: ${probe.feedCloses}, помилки сокета: ${probe.feedErrors}`);
  }

  const lost = probe.lostTrades || [];
  if (lost.length === 0) {
    lines.push(
      "",
      (probe.coveragePct ?? 100) < 100
        ? "Перелік загублених угод для цієї перевірки не зберігся."
        : "Загублених угод не було.",
    );
    return [lines.join("\n")];
  }

  // Telegram caps a message at 4096 characters. Keep each trade row intact,
  // split the log into as many <pre> blocks as needed, and never truncate it.
  const trades = probe.trades || [];
  const pattern = describeLossPattern(trades);
  lines.push("", "<b>Угоди</b> (✓ дійшла · ✗ загублена)");
  const priceWidth = Math.max(...trades.map((trade) => String(trade.price).length));
  const rows = trades.map((trade) =>
    `${trade.delivered ? "✓" : "✗"} ${formatTimeWithSeconds(trade.exchangeAtMs, timeZone)}  ` +
      `${String(trade.price).padStart(priceWidth)}  ${trade.quantity} ${trade.side}`,
  );
  const blocks = chunkRowsByLength(rows, 3_500).map((chunk) => `<pre>${chunk.join("\n")}</pre>`);
  const patternChunks = chunkTextByLength(pattern, TELEGRAM_MESSAGE_LIMIT);

  const header = lines.join("\n");
  const singleMessage = `${header}\n${blocks[0]}\n\n${patternChunks[0]}`;
  if (blocks.length === 1 && patternChunks.length === 1 && singleMessage.length <= TELEGRAM_MESSAGE_LIMIT) {
    return [singleMessage];
  }

  const messages = [header, ...blocks];
  const remainingPattern = [...patternChunks];
  const lastWithPattern = `${messages[messages.length - 1]}\n\n${remainingPattern[0]}`;
  if (lastWithPattern.length <= TELEGRAM_MESSAGE_LIMIT) {
    messages[messages.length - 1] = lastWithPattern;
    remainingPattern.shift();
  }
  messages.push(...remainingPattern);
  return messages;
}

export function chunkRowsByLength(rows, maxLength) {
  const chunks = [];
  let current = [];
  let length = 0;
  for (const row of rows) {
    if (current.length > 0 && length + row.length + 1 > maxLength) {
      chunks.push(current);
      current = [];
      length = 0;
    }
    current.push(row);
    length += row.length + 1;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

export function chunkTextByLength(text, maxLength) {
  if (text.length <= maxLength) return [text];
  const chunks = [];
  let current = "";
  for (const part of text.split(/(?<=;)\s+/u)) {
    if (part.length > maxLength) {
      if (current) {
        chunks.push(current);
        current = "";
      }
      for (let offset = 0; offset < part.length; offset += maxLength) {
        chunks.push(part.slice(offset, offset + maxLength));
      }
      continue;
    }
    const candidate = current ? `${current} ${part}` : part;
    if (candidate.length > maxLength) {
      chunks.push(current);
      current = part;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}

function detailsHeadline(probe) {
  if (probe.verdict === "ok") return "сервер працював нормально";
  if (probe.note === "kraken_disconnected") return "Kraken розірвав з’єднання";
  if (probe.note === "kraken_unavailable") return "Kraken був недоступний";
  if (probe.note === "kraken_parse_failure") return "повідомлення Kraken не вдалося розібрати";
  if (probe.verdict === "inconclusive") return "ринок був надто тихий, щоб оцінити";
  const label = {
    connect_failed: "сервер не відповідав",
    feed_silent: "угоди не доходили",
    no_matches: "дані не збігалися з біржею",
    invalid_feed_messages: "фід надсилав пошкоджені дані",
    missing_trades: "частина угод губилася",
    slow_delivery: "угоди доходили із запізненням",
    socket_dropped: "сервер обривав з'єднання",
  }[probe.note];
  return label || (probe.verdict === "down" ? "сервер не працював" : "сервер працював з перебоями");
}

function describeLossPattern(trades) {
  const analysis = analyzeLossRuns(trades);
  if (analysis.runs.length === 0) return "Загублених угод не було.";
  const positionLabel = {
    all: "протягом усього вікна",
    start: "на початку вікна",
    middle: "посередині вікна",
    end: "наприкінці вікна",
  };
  const descriptions = analysis.runs.map((run) => {
    const duration = run.durationMs > 0 ? ` за ${formatDelay(run.durationMs)}` : "";
    return `${run.count} ${tradesWord(run.count)} ${positionLabel[run.position]}${duration}`;
  });
  return analysis.runs.length === 1
    ? `Втрати утворили одну безперервну серію: ${descriptions[0]}.`
    : `Серії втрат (${analysis.runs.length}): ${descriptions.join("; ")}.`;
}

function defaultInconclusiveDetail(probe) {
  if (probe.note === "kraken_disconnected") {
    return "Kraken розірвав з’єднання під час перевірки. Еталонний потік неповний, тому стан BitcoinTicker оцінити неможливо.";
  }
  if (probe.note === "kraken_unavailable") {
    return "Біржа Kraken була недоступна, тому стан BitcoinTicker оцінити неможливо.";
  }
  if (probe.note === "kraken_parse_failure") {
    return "Kraken надіслав пошкоджене або неочікуване повідомлення під час вікна. Еталон неповний, тому стан BitcoinTicker оцінити неможливо.";
  }
  return "За час перевірки Kraken не передав жодної угоди, тому порівнювати нема чого. Спробуйте /check трохи пізніше.";
}

function verdictEmoji(verdict) {
  if (verdict === "ok") return "🟢";
  if (verdict === "degraded") return "🟠";
  if (verdict === "down") return "🔴";
  return "⚪";
}

function referenceCount(probe) {
  return probe.referenceTrades ?? probe.matched + missingCount(probe);
}

function missingCount(probe) {
  if (probe.coveragePct === null || probe.coveragePct === 0) return probe.krakenTrades;
  return Math.max(0, Math.round((probe.matched * 100) / probe.coveragePct) - probe.matched);
}

function formatDelay(ms) {
  if (ms === null || ms === undefined) return "невідомо";
  if (ms < 10) return "менш ніж 0,01 с";
  if (ms < 1000) return `${(ms / 1000).toFixed(2).replace(".", ",")} с`;
  if (ms < 10_000) return `${(ms / 1000).toFixed(1).replace(".", ",")} с`;
  return `${Math.round(ms / 1000)} с`;
}

function tradesWord(count) {
  const mod10 = count % 10;
  const mod100 = count % 100;
  if (mod10 === 1 && mod100 !== 11) return "угоду";
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return "угоди";
  return "угод";
}

function formatTimeWithSeconds(ms, timeZone) {
  return new Intl.DateTimeFormat("uk-UA", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(ms));
}

function formatDateTime(value, timeZone) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

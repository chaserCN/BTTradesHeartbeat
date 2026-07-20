import {
  computeTradeMetrics,
  parseFeedTrade,
  parseKrakenMessage,
  summarizeTradeResults,
} from "./heartbeat-core.mjs";

export function collectSession(options, dependencies = {}) {
  const {
    windowMs,
    warmupMs,
    drainMs,
    connectTimeoutMs,
    feedUrl,
    feedChannel,
    krakenUrl,
    krakenSymbol,
  } = options;
  const WebSocketClass = dependencies.WebSocketClass ?? globalThis.WebSocket;
  const wallNow = dependencies.wallNow ?? Date.now;
  const monoNow = dependencies.monoNow ?? (() => performance.now());
  const setTimer = dependencies.setTimer ?? setTimeout;
  const clearTimer = dependencies.clearTimer ?? clearTimeout;
  const logError = dependencies.logError ?? ((message) => console.error(message));

  return new Promise((resolve) => {
    const session = emptySession();
    const connectionStartedAtMs = wallNow();
    const connectionStartedMonoMs = monoNow();
    session.startedAtMs = connectionStartedAtMs;
    session.startedMonoMs = connectionStartedMonoMs;
    let finished = false;
    let phase = "connecting_feed";
    let feedSocket = null;
    let krakenSocket = null;
    let feedConnectTimer = null;
    let warmupTimer = null;
    let krakenConnectTimer = null;
    let syncTimer = null;
    let measurementTimer = null;
    let drainTimer = null;

    const recordSocketEvent = (source, eventType, details = {}, times = {}) => {
      const events = session[source].socketEvents;
      events.push({
        sequence: events.length + 1,
        eventType,
        phase,
        occurredAtMs: times.occurredAtMs ?? wallNow(),
        occurredMonoMs: times.occurredMonoMs ?? monoNow(),
        code: details.code ?? null,
        reason: details.reason ?? null,
        wasClean: details.wasClean ?? null,
        detail: details.detail ?? null,
      });
    };

    const finish = () => {
      if (finished) return;
      finished = true;
      phase = "finished";
      for (const timer of [feedConnectTimer, warmupTimer, krakenConnectTimer, syncTimer, measurementTimer, drainTimer]) {
        if (timer !== null) clearTimer(timer);
      }
      session.endedAtMs = wallNow();
      session.endedAtMonoMs = monoNow();
      if (session.measurementStartedMonoMs !== null) {
        session.windowMs =
          (session.referenceEndedMonoMs ?? session.endedAtMonoMs) - session.measurementStartedMonoMs;
      }
      for (const socket of [feedSocket, krakenSocket]) {
        try { socket?.close(); } catch { /* already closed */ }
      }
      resolve(session);
    };

    const beginMeasurement = () => {
      if (finished || phase !== "syncing") return;

      // Keep the overlapping sync history. Exact monotonic boundaries select
      // the measurement reference later; the feed pre-roll remains available
      // for copies that reached this process before Kraken did.
      session.measurementStartedAtMs = wallNow();
      session.measurementStartedMonoMs = monoNow();
      phase = "measuring";

      measurementTimer = setTimer(() => {
        if (finished || phase !== "measuring") return;
        session.referenceEndedAtMs = wallNow();
        session.referenceEndedMonoMs = monoNow();
        phase = "draining";

        // Keep Kraken open during drain as non-verdict context. Post-window
        // Kraken trades reserve their own equal-value feed copies.
        drainTimer = setTimer(finish, drainMs);
      }, windowMs);
    };

    const connectKraken = () => {
      if (finished) return;
      phase = "connecting_kraken";

      try {
        krakenSocket = new WebSocketClass(krakenUrl);
      } catch (error) {
        recordSocketEvent("kraken", "constructor_error", { detail: diagnosticDetail(error) });
        session.kraken.connectFailed = true;
        finish();
        return;
      }

      // Includes both the WebSocket handshake and subscription acknowledgement.
      krakenConnectTimer = setTimer(() => {
        if (phase === "connecting_kraken") {
          recordSocketEvent("kraken", "timeout");
          session.kraken.connectFailed = true;
          finish();
        }
      }, connectTimeoutMs);

      krakenSocket.onopen = () => {
        if (finished) return;
        recordSocketEvent("kraken", "open");
        session.kraken.connected = true;
        try {
          krakenSocket.send(JSON.stringify({
            method: "subscribe",
            params: { channel: "trade", symbol: [krakenSymbol], snapshot: false },
          }));
        } catch (error) {
          recordSocketEvent("kraken", "subscribe_send_error", { detail: diagnosticDetail(error) });
          session.kraken.connectFailed = true;
          finish();
          return;
        }
        recordSocketEvent("kraken", "subscribe_sent");
      };
      krakenSocket.onmessage = (event) => {
        if (finished) return;
        const receivedAtMs = wallNow();
        const receivedMonoMs = monoNow();
        session.kraken.messages += 1;
        const message = parseKrakenMessage(event.data, receivedAtMs, receivedMonoMs);
        session.kraken.parseFailures += message.parseFailures;
        session.kraken.events.push({
          sequence: session.kraken.messages,
          phase,
          receivedAtMs,
          receivedMonoMs,
          parsedTrades: message.trades.length,
          parseFailures: message.parseFailures,
          rawPreview: message.parseFailures > 0 ? previewRawMessage(event.data) : null,
        });
        if (message.subscription === "accepted" && phase === "connecting_kraken") {
          clearTimer(krakenConnectTimer);
          session.syncStartedAtMs = receivedAtMs;
          session.syncStartedMonoMs = receivedMonoMs;
          session.kraken.subscribedAtMs = receivedAtMs;
          session.kraken.subscribedAtMonoMs = receivedMonoMs;
          recordSocketEvent("kraken", "subscribe_accepted", {}, {
            occurredAtMs: receivedAtMs,
            occurredMonoMs: receivedMonoMs,
          });
          phase = "syncing";
          syncTimer = setTimer(beginMeasurement, warmupMs);
          return;
        }
        if (message.subscription === "rejected") {
          recordSocketEvent("kraken", "subscribe_rejected", {
            detail: diagnosticDetail(message.error),
          }, {
            occurredAtMs: receivedAtMs,
            occurredMonoMs: receivedMonoMs,
          });
          session.kraken.connectFailed = true;
          logError(`Kraken subscription failed: ${message.error || "unknown error"}`);
          finish();
          return;
        }
        if (phase === "syncing" || phase === "measuring" || phase === "draining") {
          session.kraken.trades.push(...message.trades.map((trade) => ({ ...trade, phase })));
        }
      };
      krakenSocket.onerror = (event) => {
        if (finished) return;
        session.kraken.errors += 1;
        recordSocketEvent("kraken", "error", { detail: diagnosticDetail(event?.message ?? event?.error) });
        if (phase === "connecting_kraken") {
          session.kraken.connectFailed = true;
          finish();
        } else if (phase === "syncing" || phase === "measuring") {
          session.kraken.disconnected = true;
          finish();
        }
      };
      krakenSocket.onclose = (event) => {
        if (finished) return;
        session.kraken.closes += 1;
        recordSocketEvent("kraken", "close", closeDetails(event));
        if (phase === "draining") return;
        if (phase === "connecting_kraken") {
          session.kraken.connectFailed = true;
        } else if (phase === "syncing" || phase === "measuring") {
          session.kraken.disconnected = true;
        }
        finish();
      };
    };

    try {
      feedSocket = new WebSocketClass(feedUrl);
    } catch (error) {
      recordSocketEvent("feed", "constructor_error", { detail: diagnosticDetail(error) });
      session.feed.connectFailed = true;
      finish();
      return;
    }

    feedConnectTimer = setTimer(() => {
      if (session.feed.handshakeMs === null) {
        recordSocketEvent("feed", "timeout");
        session.feed.connectFailed = true;
        finish();
      }
    }, connectTimeoutMs);

    feedSocket.onopen = () => {
      if (finished) return;
      clearTimer(feedConnectTimer);
      const openedAtMs = wallNow();
      const openedMonoMs = monoNow();
      session.feed.handshakeMs = Math.round(openedMonoMs - connectionStartedMonoMs);
      recordSocketEvent("feed", "open", {}, {
        occurredAtMs: openedAtMs,
        occurredMonoMs: openedMonoMs,
      });
      session.feed.subscribedAtMs = wallNow();
      session.feed.subscribedAtMonoMs = monoNow();
      try {
        feedSocket.send(JSON.stringify({ subscribe: feedChannel }));
      } catch (error) {
        recordSocketEvent("feed", "subscribe_send_error", { detail: diagnosticDetail(error) });
        session.feed.connectFailed = true;
        finish();
        return;
      }
      recordSocketEvent("feed", "subscribe_sent", {}, {
        occurredAtMs: session.feed.subscribedAtMs,
        occurredMonoMs: session.feed.subscribedAtMonoMs,
      });
      phase = "warming_feed";
      warmupTimer = setTimer(connectKraken, warmupMs);
    };
    feedSocket.onmessage = (event) => {
      if (finished) return;
      const receivedAtMs = wallNow();
      const receivedMonoMs = monoNow();
      session.feed.messages += 1;
      if (session.feed.subscribeToFirstMessageMs === null && session.feed.subscribedAtMonoMs !== null) {
        session.feed.firstMessageAtMs = receivedAtMs;
        session.feed.firstMessageAtMonoMs = receivedMonoMs;
        session.feed.subscribeToFirstMessageMs = Math.round(receivedMonoMs - session.feed.subscribedAtMonoMs);
      }
      const parsed = parseFeedTrade(event.data, receivedAtMs, receivedMonoMs);
      session.feed.parseFailures += parsed.parseFailures;
      session.feed.events.push({
        sequence: session.feed.messages,
        phase,
        receivedAtMs,
        receivedMonoMs,
        parsedTrades: parsed.trade ? 1 : 0,
        parseFailures: parsed.parseFailures,
        rawPreview: parsed.parseFailures > 0 ? previewRawMessage(event.data) : null,
      });
      if (parsed.trade) {
        if (session.feed.subscribeToFirstTradeMs === null && session.feed.subscribedAtMonoMs !== null) {
          session.feed.firstTradeAtMs = receivedAtMs;
          session.feed.firstTradeAtMonoMs = receivedMonoMs;
          session.feed.subscribeToFirstTradeMs = Math.round(receivedMonoMs - session.feed.subscribedAtMonoMs);
        }
        session.feed.trades.push({ ...parsed.trade, phase, sequence: session.feed.messages });
      }
    };
    feedSocket.onerror = (event) => {
      if (finished) return;
      session.feed.errors += 1;
      recordSocketEvent("feed", "error", { detail: diagnosticDetail(event?.message ?? event?.error) });
      if (isPreMeasurementPhase(phase)) {
        session.feed.connectFailed = true;
        finish();
      }
    };
    feedSocket.onclose = (event) => {
      if (finished) return;
      session.feed.closes += 1;
      recordSocketEvent("feed", "close", closeDetails(event));
      if (isPreMeasurementPhase(phase)) {
        session.feed.connectFailed = true;
        finish();
      }
    };
  });
}

export function computeSessionMetrics(session, options) {
  const { preRollMs, drainMs } = options;
  const monoNow = options.monoNow ?? (() => performance.now());
  if (session.measurementStartedMonoMs === null) {
    return {
      ...computeTradeMetrics([], []),
      feedCandidates: [],
      sync: computeTradeMetrics([], []),
      drain: computeTradeMetrics([], []),
      deliveryHorizonFeedMessages: 0,
      deliveryHorizonFeedParseFailures: 0,
      referenceWindowKrakenParseFailures: 0,
    };
  }

  const referenceEnd = session.referenceEndedMonoMs ?? session.endedAtMonoMs ?? monoNow();
  const contextStart = session.syncStartedMonoMs ?? session.measurementStartedMonoMs;
  const contextEnd = session.endedAtMonoMs ?? monoNow();
  const contextReference = session.kraken.trades.filter(
    (trade) => trade.receivedMonoMs >= contextStart && trade.receivedMonoMs <= contextEnd,
  );
  const contextFeedCandidates = session.feed.trades.filter(
    (trade) => trade.receivedMonoMs >= contextStart - preRollMs && trade.receivedMonoMs <= contextEnd,
  );
  const context = computeTradeMetrics(contextReference, contextFeedCandidates, {
    maxLeadMs: preRollMs,
    maxLagMs: drainMs,
  });
  const measurementTrades = context.allTrades.filter(
    (trade) => trade.receivedMonoMs >= session.measurementStartedMonoMs && trade.receivedMonoMs < referenceEnd,
  );
  const feedCandidates = session.feed.trades.filter(
    (trade) =>
      trade.receivedMonoMs >= session.measurementStartedMonoMs - preRollMs &&
      trade.receivedMonoMs <= referenceEnd + drainMs,
  );
  const syncTrades = context.allTrades.filter(
    (trade) => trade.receivedMonoMs < session.measurementStartedMonoMs,
  );
  const drainTrades = context.allTrades.filter((trade) => trade.receivedMonoMs >= referenceEnd);
  const deliveryHorizonFeedEvents = session.feed.events.filter(
    (event) => event.receivedMonoMs >= session.measurementStartedMonoMs && event.receivedMonoMs <= contextEnd,
  );
  const referenceWindowKrakenEvents = session.kraken.events.filter(
    (event) => event.receivedMonoMs >= session.measurementStartedMonoMs && event.receivedMonoMs < referenceEnd,
  );
  return {
    ...summarizeTradeResults(measurementTrades),
    feedCandidates,
    sync: summarizeTradeResults(syncTrades),
    drain: summarizeTradeResults(drainTrades),
    deliveryHorizonFeedMessages: deliveryHorizonFeedEvents.length,
    deliveryHorizonFeedParseFailures: deliveryHorizonFeedEvents.reduce(
      (sum, event) => sum + event.parseFailures,
      0,
    ),
    referenceWindowKrakenParseFailures: referenceWindowKrakenEvents.reduce(
      (sum, event) => sum + event.parseFailures,
      0,
    ),
  };
}

export function previewRawMessage(raw) {
  const text = typeof raw === "string"
    ? raw
    : raw instanceof ArrayBuffer
      ? Buffer.from(raw).toString("utf8")
      : ArrayBuffer.isView(raw)
        ? Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength).toString("utf8")
        : String(raw);
  return text.slice(0, 500);
}

function closeDetails(event) {
  return {
    code: Number.isInteger(event?.code) ? event.code : null,
    reason: diagnosticDetail(event?.reason),
    wasClean: typeof event?.wasClean === "boolean" ? event.wasClean : null,
  };
}

function diagnosticDetail(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = value instanceof Error ? value.message : String(value);
  return text.slice(0, 500);
}

function emptySession() {
  return {
    startedAtMs: null,
    startedMonoMs: null,
    windowMs: 0,
    measurementStartedAtMs: null,
    measurementStartedMonoMs: null,
    referenceEndedAtMs: null,
    referenceEndedMonoMs: null,
    endedAtMs: null,
    endedAtMonoMs: null,
    syncStartedAtMs: null,
    syncStartedMonoMs: null,
    feed: {
      handshakeMs: null,
      connectFailed: false,
      trades: [],
      closes: 0,
      errors: 0,
      messages: 0,
      parseFailures: 0,
      events: [],
      socketEvents: [],
      subscribedAtMs: null,
      subscribedAtMonoMs: null,
      firstMessageAtMs: null,
      firstMessageAtMonoMs: null,
      firstTradeAtMs: null,
      firstTradeAtMonoMs: null,
      subscribeToFirstMessageMs: null,
      subscribeToFirstTradeMs: null,
    },
    kraken: {
      connected: false,
      connectFailed: false,
      disconnected: false,
      trades: [],
      messages: 0,
      parseFailures: 0,
      events: [],
      socketEvents: [],
      subscribedAtMs: null,
      subscribedAtMonoMs: null,
      closes: 0,
      errors: 0,
    },
  };
}

function isPreMeasurementPhase(phase) {
  return phase === "connecting_feed" ||
    phase === "warming_feed" ||
    phase === "connecting_kraken" ||
    phase === "syncing";
}

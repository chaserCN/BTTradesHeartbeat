import { DatabaseSync } from "node:sqlite";

export const SCHEMA_VERSION = 7;

const PROBE_COLUMNS = [
  ["at", "at"],
  ["started_at_ms", "startedAtMs"],
  ["started_mono_ms", "startedMonoMs"],
  ["feed_subscribed_at_ms", "feedSubscribedAtMs"],
  ["feed_subscribed_mono_ms", "feedSubscribedMonoMs"],
  ["first_feed_message_at_ms", "firstFeedMessageAtMs"],
  ["first_feed_message_mono_ms", "firstFeedMessageMonoMs"],
  ["first_feed_trade_at_ms", "firstFeedTradeAtMs"],
  ["first_feed_trade_mono_ms", "firstFeedTradeMonoMs"],
  ["kraken_subscribed_at_ms", "krakenSubscribedAtMs"],
  ["kraken_subscribed_mono_ms", "krakenSubscribedMonoMs"],
  ["measurement_started_at_ms", "measurementStartedAtMs"],
  ["measurement_started_mono_ms", "measurementStartedMonoMs"],
  ["measurement_ended_at_ms", "measurementEndedAtMs"],
  ["measurement_ended_mono_ms", "measurementEndedMonoMs"],
  ["session_ended_at_ms", "sessionEndedAtMs"],
  ["session_ended_mono_ms", "sessionEndedMonoMs"],
  ["verdict", "verdict"],
  ["note", "note"],
  ["window_ms", "windowMs"],
  ["handshake_ms", "handshakeMs"],
  ["feed_messages", "feedMessages"],
  ["feed_parse_failures", "feedParseFailures"],
  ["delivery_horizon_feed_messages", "deliveryHorizonFeedMessages"],
  ["delivery_horizon_feed_parse_failures", "deliveryHorizonFeedParseFailures"],
  ["feed_parsed_trades", "feedParsedTrades"],
  ["feed_pre_measurement_trades", "feedPreMeasurementTrades"],
  ["feed_sync_trades", "feedSyncTrades"],
  ["feed_measurement_trades", "feedMeasurementTrades"],
  ["feed_drain_trades", "feedDrainTrades"],
  ["kraken_messages", "krakenMessages"],
  ["kraken_parse_failures", "krakenParseFailures"],
  ["reference_window_kraken_parse_failures", "referenceWindowKrakenParseFailures"],
  ["kraken_sync_trades", "krakenSyncTrades"],
  ["kraken_drain_trades", "krakenDrainTrades"],
  ["sync_matched", "syncMatched"],
  ["sync_coverage_pct", "syncCoveragePct"],
  ["feed_candidate_trades", "feedCandidateTrades"],
  ["reference_trades", "referenceTrades"],
  ["matched", "matched"],
  ["coverage_pct", "coveragePct"],
  ["delay_median_ms", "delayMedianMs"],
  ["delay_p90_ms", "delayP90Ms"],
  ["delay_max_ms", "delayMaxMs"],
  ["signed_delay_min_ms", "signedDelayMinMs"],
  ["signed_delay_median_ms", "signedDelayMedianMs"],
  ["feed_closes", "feedCloses"],
  ["feed_errors", "feedErrors"],
  ["kraken_closes", "krakenCloses"],
  ["kraken_errors", "krakenErrors"],
];

export function createHeartbeatStore(dbFile) {
  const db = new DatabaseSync(dbFile);
  db.exec("PRAGMA journal_mode = WAL;");
  const schemaVersion = db.prepare("PRAGMA user_version").get().user_version;
  if (schemaVersion !== SCHEMA_VERSION) {
    db.exec(`
      DROP TABLE IF EXISTS socket_events;
      DROP TABLE IF EXISTS message_events;
      DROP TABLE IF EXISTS feed_trades;
      DROP TABLE IF EXISTS trades;
      DROP TABLE IF EXISTS probes;
    `);
  }
  db.exec(schemaSql());

  let readOnlyDb = null;

  function recordProbe(probe) {
    db.exec("BEGIN");
    try {
      const columnNames = PROBE_COLUMNS.map(([column]) => column);
      const values = PROBE_COLUMNS.map(([, property]) => probe[property] ?? null);
      const info = db.prepare(`
        INSERT INTO probes (${columnNames.join(", ")})
        VALUES (${columnNames.map(() => "?").join(", ")})
      `).run(...values);
      probe.id = Number(info.lastInsertRowid);
      insertReferenceTrades(probe.id, "measurement", probe.trades || []);
      insertReferenceTrades(probe.id, "sync", probe.syncTrades || []);
      insertReferenceTrades(probe.id, "drain", probe.drainTrades || []);
      insertFeedTrades(probe.id, probe.feedTrades || []);
      insertMessageEvents(probe.id, probe.messageEvents || []);
      insertSocketEvents(probe.id, probe.socketEvents || []);
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      delete probe.id;
      throw error;
    }
  }

  function insertReferenceTrades(probeId, scope, trades) {
    const statement = db.prepare(`
      INSERT INTO trades (
        probe_id, scope, exchange_at_ms, kraken_received_at_ms, kraken_received_mono_ms, price, quantity, side,
        kraken_trade_id, delivered, feed_received_at_ms, signed_delay_ms, delay_ms, matched_feed_sequence
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const trade of trades) {
      statement.run(
        probeId, scope, trade.exchangeAtMs, trade.receivedAtMs, trade.receivedMonoMs, trade.price, trade.quantity,
        trade.side, trade.tradeId ?? null, trade.delivered ? 1 : 0, trade.feedReceivedAtMs ?? null,
        trade.signedDelayMs ?? null, trade.delayMs ?? null, trade.matchedFeedSequence ?? null,
      );
    }
  }

  function insertFeedTrades(probeId, trades) {
    const statement = db.prepare(`
      INSERT INTO feed_trades (
        probe_id, sequence, phase, exchange_at_ms, received_at_ms, received_mono_ms,
        price, quantity, side, matched_scope
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const trade of trades) {
      statement.run(
        probeId, trade.sequence, trade.phase, trade.exchangeAtMs, trade.receivedAtMs, trade.receivedMonoMs,
        trade.price, trade.quantity, trade.side, trade.matchedScope ?? null,
      );
    }
  }

  function insertMessageEvents(probeId, events) {
    const statement = db.prepare(`
      INSERT INTO message_events (
        probe_id, source, sequence, phase, received_at_ms, received_mono_ms,
        parsed_trades, parse_failures, raw_preview
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const event of events) {
      statement.run(
        probeId, event.source, event.sequence, event.phase, event.receivedAtMs, event.receivedMonoMs,
        event.parsedTrades, event.parseFailures, event.rawPreview ?? null,
      );
    }
  }

  function insertSocketEvents(probeId, events) {
    const statement = db.prepare(`
      INSERT INTO socket_events (
        probe_id, source, sequence, event_type, phase, occurred_at_ms, occurred_mono_ms,
        code, reason, was_clean, detail
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const event of events) {
      statement.run(
        probeId, event.source, event.sequence, event.eventType, event.phase,
        event.occurredAtMs, event.occurredMonoMs, event.code ?? null, event.reason ?? null,
        event.wasClean === null || event.wasClean === undefined ? null : event.wasClean ? 1 : 0,
        event.detail ?? null,
      );
    }
  }

  function getLastProbe() {
    return rowToProbe(db.prepare("SELECT * FROM probes ORDER BY id DESC LIMIT 1").get());
  }

  function getProbeById(id) {
    return rowToProbe(db.prepare("SELECT * FROM probes WHERE id = ?").get(id));
  }

  function getProbesSince(sinceMs) {
    return db
      .prepare("SELECT * FROM probes WHERE at >= ? ORDER BY id")
      .all(new Date(sinceMs).toISOString())
      .map(rowToProbe);
  }

  function getTrades(probeId, scope = "measurement") {
    return db.prepare(`SELECT
        exchange_at_ms, kraken_received_at_ms, kraken_received_mono_ms, price, quantity, side, kraken_trade_id,
        delivered, feed_received_at_ms, signed_delay_ms, delay_ms, matched_feed_sequence
        FROM trades WHERE probe_id = ? AND scope = ? ORDER BY exchange_at_ms, kraken_received_at_ms, id`)
      .all(probeId, scope)
      .map((row) => ({
        exchangeAtMs: row.exchange_at_ms,
        receivedAtMs: row.kraken_received_at_ms,
        receivedMonoMs: row.kraken_received_mono_ms,
        price: row.price,
        quantity: row.quantity,
        side: row.side,
        tradeId: row.kraken_trade_id,
        delivered: row.delivered === 1,
        feedReceivedAtMs: row.feed_received_at_ms,
        signedDelayMs: row.signed_delay_ms,
        delayMs: row.delay_ms,
        matchedFeedSequence: row.matched_feed_sequence,
      }));
  }

  function getFeedTrades(probeId) {
    return db.prepare(`SELECT sequence, phase, exchange_at_ms, received_at_ms, received_mono_ms,
          price, quantity, side, matched_scope
        FROM feed_trades WHERE probe_id = ? ORDER BY sequence`)
      .all(probeId)
      .map((row) => ({
        sequence: row.sequence,
        phase: row.phase,
        exchangeAtMs: row.exchange_at_ms,
        receivedAtMs: row.received_at_ms,
        receivedMonoMs: row.received_mono_ms,
        price: row.price,
        quantity: row.quantity,
        side: row.side,
        matchedScope: row.matched_scope,
      }));
  }

  function getMessageEvents(probeId) {
    return db.prepare(`SELECT source, sequence, phase, received_at_ms, received_mono_ms,
          parsed_trades, parse_failures, raw_preview
        FROM message_events WHERE probe_id = ? ORDER BY received_at_ms, id`)
      .all(probeId)
      .map((row) => ({
        source: row.source,
        sequence: row.sequence,
        phase: row.phase,
        receivedAtMs: row.received_at_ms,
        receivedMonoMs: row.received_mono_ms,
        parsedTrades: row.parsed_trades,
        parseFailures: row.parse_failures,
        rawPreview: row.raw_preview,
      }));
  }

  function getSocketEvents(probeId) {
    return db.prepare(`SELECT source, sequence, event_type, phase, occurred_at_ms, occurred_mono_ms,
          code, reason, was_clean, detail
        FROM socket_events WHERE probe_id = ? ORDER BY occurred_mono_ms, id`)
      .all(probeId)
      .map((row) => ({
        source: row.source,
        sequence: row.sequence,
        eventType: row.event_type,
        phase: row.phase,
        occurredAtMs: row.occurred_at_ms,
        occurredMonoMs: row.occurred_mono_ms,
        code: row.code,
        reason: row.reason,
        wasClean: row.was_clean === null ? null : row.was_clean === 1,
        detail: row.detail,
      }));
  }

  function withTrades(probe) {
    if (!probe) return null;
    const trades = getTrades(probe.id);
    const syncTrades = getTrades(probe.id, "sync");
    const drainTrades = getTrades(probe.id, "drain");
    const feedTrades = getFeedTrades(probe.id);
    const messageEvents = getMessageEvents(probe.id);
    return {
      ...probe,
      trades,
      syncTrades,
      drainTrades,
      feedTrades,
      messageEvents,
      socketEvents: getSocketEvents(probe.id),
      phaseCounts: buildPhaseCounts({ trades, syncTrades, drainTrades, feedTrades, messageEvents }),
      lostTrades: trades.filter((trade) => !trade.delivered),
    };
  }

  function kvGet(key) {
    return db.prepare("SELECT value FROM kv WHERE key = ?").get(key)?.value;
  }

  function kvSet(key, value) {
    db.prepare(
      "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    ).run(key, String(value));
  }

  function getReadOnlyDb() {
    if (!readOnlyDb) readOnlyDb = new DatabaseSync(dbFile, { readOnly: true });
    return readOnlyDb;
  }

  function close() {
    readOnlyDb?.close();
    db.close();
  }

  return {
    close,
    getFeedTrades,
    getLastProbe,
    getMessageEvents,
    getProbeById,
    getProbesSince,
    getReadOnlyDb,
    getSocketEvents,
    getTrades,
    kvGet,
    kvSet,
    recordProbe,
    withTrades,
  };
}

export function rowToProbe(row) {
  if (!row) return null;
  return {
    id: row.id,
    at: row.at,
    startedAtMs: row.started_at_ms,
    startedMonoMs: row.started_mono_ms,
    feedSubscribedAtMs: row.feed_subscribed_at_ms,
    feedSubscribedMonoMs: row.feed_subscribed_mono_ms,
    firstFeedMessageAtMs: row.first_feed_message_at_ms,
    firstFeedMessageMonoMs: row.first_feed_message_mono_ms,
    firstFeedTradeAtMs: row.first_feed_trade_at_ms,
    firstFeedTradeMonoMs: row.first_feed_trade_mono_ms,
    krakenSubscribedAtMs: row.kraken_subscribed_at_ms,
    krakenSubscribedMonoMs: row.kraken_subscribed_mono_ms,
    measurementStartedAt: toIsoOrNull(row.measurement_started_at_ms),
    measurementStartedAtMs: row.measurement_started_at_ms,
    measurementStartedMonoMs: row.measurement_started_mono_ms,
    measurementEndedAt: toIsoOrNull(row.measurement_ended_at_ms),
    measurementEndedAtMs: row.measurement_ended_at_ms,
    measurementEndedMonoMs: row.measurement_ended_mono_ms,
    sessionEndedAt: toIsoOrNull(row.session_ended_at_ms),
    sessionEndedAtMs: row.session_ended_at_ms,
    sessionEndedMonoMs: row.session_ended_mono_ms,
    verdict: row.verdict,
    note: row.note ?? "",
    windowMs: row.window_ms,
    windowSeconds: row.window_ms === null ? null : Math.round(row.window_ms / 1_000),
    handshakeMs: row.handshake_ms,
    subscribeToFirstMessageMs: elapsedOrNull(row.feed_subscribed_mono_ms, row.first_feed_message_mono_ms),
    subscribeToFirstTradeMs: elapsedOrNull(row.feed_subscribed_mono_ms, row.first_feed_trade_mono_ms),
    feedMessages: row.feed_messages,
    feedParseFailures: row.feed_parse_failures,
    deliveryHorizonFeedMessages: row.delivery_horizon_feed_messages,
    deliveryHorizonFeedParseFailures: row.delivery_horizon_feed_parse_failures,
    feedParsedTrades: row.feed_parsed_trades,
    feedPreMeasurementTrades: row.feed_pre_measurement_trades,
    feedSyncTrades: row.feed_sync_trades,
    feedMeasurementTrades: row.feed_measurement_trades,
    feedDrainTrades: row.feed_drain_trades,
    krakenMessages: row.kraken_messages,
    krakenParseFailures: row.kraken_parse_failures,
    referenceWindowKrakenParseFailures: row.reference_window_kraken_parse_failures,
    krakenSyncTrades: row.kraken_sync_trades,
    krakenDrainTrades: row.kraken_drain_trades,
    syncMatched: row.sync_matched,
    syncCoveragePct: row.sync_coverage_pct,
    feedCandidateTrades: row.feed_candidate_trades,
    referenceTrades: row.reference_trades,
    matched: row.matched,
    coveragePct: row.coverage_pct,
    delayMedianMs: row.delay_median_ms,
    delayP90Ms: row.delay_p90_ms,
    delayMaxMs: row.delay_max_ms,
    signedDelayMinMs: row.signed_delay_min_ms,
    signedDelayMedianMs: row.signed_delay_median_ms,
    feedCloses: row.feed_closes,
    feedErrors: row.feed_errors,
    krakenCloses: row.kraken_closes,
    krakenErrors: row.kraken_errors,
  };
}

function elapsedOrNull(start, end) {
  return start === null || start === undefined || end === null || end === undefined
    ? null
    : Math.round(end - start);
}

function toIsoOrNull(value) {
  return value === null || value === undefined ? null : new Date(value).toISOString();
}

export function buildPhaseCounts({ trades, syncTrades, drainTrades, feedTrades, messageEvents }) {
  const phases = ["syncing", "measuring", "draining"];
  const counts = Object.fromEntries(phases.map((phase) => [phase, {
    feed: { messages: 0, parsedTrades: 0, parseFailures: 0, trades: 0 },
    kraken: { messages: 0, parsedTrades: 0, parseFailures: 0, trades: 0 },
  }]));

  for (const event of messageEvents) {
    const phase = counts[event.phase];
    if (!phase || !phase[event.source]) continue;
    phase[event.source].messages += 1;
    phase[event.source].parsedTrades += event.parsedTrades;
    phase[event.source].parseFailures += event.parseFailures;
  }
  for (const trade of feedTrades) {
    if (counts[trade.phase]) counts[trade.phase].feed.trades += 1;
  }
  counts.syncing.kraken.trades = syncTrades.length;
  counts.measuring.kraken.trades = trades.length;
  counts.draining.kraken.trades = drainTrades.length;
  return counts;
}

function schemaSql() {
  return `
    CREATE TABLE IF NOT EXISTS probes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      at TEXT NOT NULL,
      started_at_ms INTEGER,
      started_mono_ms REAL,
      feed_subscribed_at_ms INTEGER,
      feed_subscribed_mono_ms REAL,
      first_feed_message_at_ms INTEGER,
      first_feed_message_mono_ms REAL,
      first_feed_trade_at_ms INTEGER,
      first_feed_trade_mono_ms REAL,
      kraken_subscribed_at_ms INTEGER,
      kraken_subscribed_mono_ms REAL,
      measurement_started_at_ms INTEGER,
      measurement_started_mono_ms REAL,
      measurement_ended_at_ms INTEGER,
      measurement_ended_mono_ms REAL,
      session_ended_at_ms INTEGER,
      session_ended_mono_ms REAL,
      verdict TEXT NOT NULL,
      note TEXT,
      window_ms REAL,
      handshake_ms INTEGER,
      feed_messages INTEGER,
      feed_parse_failures INTEGER,
      delivery_horizon_feed_messages INTEGER,
      delivery_horizon_feed_parse_failures INTEGER,
      feed_parsed_trades INTEGER,
      feed_pre_measurement_trades INTEGER,
      feed_sync_trades INTEGER,
      feed_measurement_trades INTEGER,
      feed_drain_trades INTEGER,
      kraken_messages INTEGER,
      kraken_parse_failures INTEGER,
      reference_window_kraken_parse_failures INTEGER,
      kraken_sync_trades INTEGER,
      kraken_drain_trades INTEGER,
      sync_matched INTEGER,
      sync_coverage_pct INTEGER,
      feed_candidate_trades INTEGER,
      reference_trades INTEGER,
      matched INTEGER,
      coverage_pct INTEGER,
      delay_median_ms INTEGER,
      delay_p90_ms INTEGER,
      delay_max_ms INTEGER,
      signed_delay_min_ms INTEGER,
      signed_delay_median_ms INTEGER,
      feed_closes INTEGER,
      feed_errors INTEGER,
      kraken_closes INTEGER,
      kraken_errors INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_probes_at ON probes(at);
    CREATE TABLE IF NOT EXISTS trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      probe_id INTEGER NOT NULL REFERENCES probes(id),
      scope TEXT NOT NULL,
      exchange_at_ms REAL NOT NULL,
      kraken_received_at_ms INTEGER NOT NULL,
      kraken_received_mono_ms REAL NOT NULL,
      price REAL NOT NULL,
      quantity REAL NOT NULL,
      side TEXT NOT NULL,
      kraken_trade_id TEXT,
      delivered INTEGER NOT NULL DEFAULT 0,
      feed_received_at_ms INTEGER,
      signed_delay_ms INTEGER,
      delay_ms INTEGER,
      matched_feed_sequence INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_trades_probe ON trades(probe_id, scope);
    CREATE TABLE IF NOT EXISTS feed_trades (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      probe_id INTEGER NOT NULL REFERENCES probes(id),
      sequence INTEGER NOT NULL,
      phase TEXT NOT NULL,
      exchange_at_ms REAL NOT NULL,
      received_at_ms INTEGER NOT NULL,
      received_mono_ms REAL NOT NULL,
      price REAL NOT NULL,
      quantity REAL NOT NULL,
      side TEXT NOT NULL,
      matched_scope TEXT,
      UNIQUE(probe_id, sequence)
    );
    CREATE INDEX IF NOT EXISTS idx_feed_trades_probe ON feed_trades(probe_id, phase);
    CREATE TABLE IF NOT EXISTS message_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      probe_id INTEGER NOT NULL REFERENCES probes(id),
      source TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      phase TEXT NOT NULL,
      received_at_ms INTEGER NOT NULL,
      received_mono_ms REAL NOT NULL,
      parsed_trades INTEGER NOT NULL,
      parse_failures INTEGER NOT NULL,
      raw_preview TEXT,
      UNIQUE(probe_id, source, sequence)
    );
    CREATE INDEX IF NOT EXISTS idx_message_events_probe ON message_events(probe_id, source, phase);
    CREATE TABLE IF NOT EXISTS socket_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      probe_id INTEGER NOT NULL REFERENCES probes(id),
      source TEXT NOT NULL,
      sequence INTEGER NOT NULL,
      event_type TEXT NOT NULL,
      phase TEXT NOT NULL,
      occurred_at_ms INTEGER NOT NULL,
      occurred_mono_ms REAL NOT NULL,
      code INTEGER,
      reason TEXT,
      was_clean INTEGER,
      detail TEXT,
      UNIQUE(probe_id, source, sequence)
    );
    CREATE INDEX IF NOT EXISTS idx_socket_events_probe ON socket_events(probe_id, source, phase);
    CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
    PRAGMA user_version = ${SCHEMA_VERSION};
  `;
}

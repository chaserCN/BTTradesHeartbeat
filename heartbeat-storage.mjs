import { DatabaseSync } from "node:sqlite";

export const SCHEMA_VERSION = 6;

export function createHeartbeatStore(dbFile) {
  const db = new DatabaseSync(dbFile);
  db.exec("PRAGMA journal_mode = WAL;");
  const schemaVersion = db.prepare("PRAGMA user_version").get().user_version;
  if (schemaVersion !== SCHEMA_VERSION) {
    db.exec(`
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
      const info = db.prepare(`
        INSERT INTO probes (
          at, measurement_started_at, measurement_started_mono_ms, verdict, note, window_seconds, handshake_ms,
          subscribe_to_first_message_ms, subscribe_to_first_trade_ms,
          feed_messages, feed_parse_failures, measurement_feed_messages, measurement_feed_parse_failures,
          feed_parsed_trades, feed_warmup_trades, feed_sync_trades,
          kraken_messages, kraken_parse_failures, measurement_kraken_parse_failures,
          kraken_sync_trades, sync_matched, sync_coverage_pct,
          kraken_trades, our_trades, reference_trades, matched, coverage_pct,
          delay_median_ms, delay_slow_ms, delay_max_ms, signed_delay_min_ms, signed_delay_median_ms,
          feed_closes, feed_errors
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        probe.at, probe.measurementStartedAt ?? null, probe.measurementStartedMonoMs ?? null,
        probe.verdict, probe.note ?? null,
        probe.windowSeconds ?? null, probe.handshakeMs ?? null,
        probe.subscribeToFirstMessageMs ?? null, probe.subscribeToFirstTradeMs ?? null,
        probe.feedMessages ?? null, probe.feedParseFailures ?? null,
        probe.measurementFeedMessages ?? null, probe.measurementFeedParseFailures ?? null,
        probe.feedParsedTrades ?? null, probe.feedWarmupTrades ?? null, probe.feedSyncTrades ?? null,
        probe.krakenMessages ?? null, probe.krakenParseFailures ?? null,
        probe.measurementKrakenParseFailures ?? null, probe.krakenSyncTrades ?? null,
        probe.syncMatched ?? null, probe.syncCoveragePct ?? null,
        probe.krakenTrades ?? null, probe.ourTrades ?? null, probe.referenceTrades ?? null,
        probe.matched ?? null, probe.coveragePct ?? null,
        probe.delayMedianMs ?? null, probe.delaySlowMs ?? null, probe.delayMaxMs ?? null,
        probe.signedDelayMinMs ?? null, probe.signedDelayMedianMs ?? null,
        probe.feedCloses ?? null, probe.feedErrors ?? null,
      );
      probe.id = Number(info.lastInsertRowid);
      insertReferenceTrades(probe.id, "measurement", probe.trades || []);
      insertReferenceTrades(probe.id, "sync", probe.syncTrades || []);
      insertReferenceTrades(probe.id, "drain", probe.drainTrades || []);
      insertFeedTrades(probe.id, probe.feedTrades || []);
      insertMessageEvents(probe.id, probe.messageEvents || []);
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

  function withTrades(probe) {
    if (!probe) return null;
    const trades = getTrades(probe.id);
    return {
      ...probe,
      trades,
      syncTrades: getTrades(probe.id, "sync"),
      drainTrades: getTrades(probe.id, "drain"),
      feedTrades: getFeedTrades(probe.id),
      messageEvents: getMessageEvents(probe.id),
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
    measurementStartedAt: row.measurement_started_at,
    measurementStartedMonoMs: row.measurement_started_mono_ms,
    verdict: row.verdict,
    note: row.note ?? "",
    windowSeconds: row.window_seconds,
    handshakeMs: row.handshake_ms,
    subscribeToFirstMessageMs: row.subscribe_to_first_message_ms,
    subscribeToFirstTradeMs: row.subscribe_to_first_trade_ms,
    feedMessages: row.feed_messages,
    feedParseFailures: row.feed_parse_failures,
    measurementFeedMessages: row.measurement_feed_messages,
    measurementFeedParseFailures: row.measurement_feed_parse_failures,
    feedParsedTrades: row.feed_parsed_trades,
    feedWarmupTrades: row.feed_warmup_trades,
    feedSyncTrades: row.feed_sync_trades,
    krakenMessages: row.kraken_messages,
    krakenParseFailures: row.kraken_parse_failures,
    measurementKrakenParseFailures: row.measurement_kraken_parse_failures,
    krakenSyncTrades: row.kraken_sync_trades,
    syncMatched: row.sync_matched,
    syncCoveragePct: row.sync_coverage_pct,
    krakenTrades: row.kraken_trades,
    ourTrades: row.our_trades,
    referenceTrades: row.reference_trades,
    matched: row.matched,
    coveragePct: row.coverage_pct,
    delayMedianMs: row.delay_median_ms,
    delaySlowMs: row.delay_slow_ms,
    delayMaxMs: row.delay_max_ms,
    signedDelayMinMs: row.signed_delay_min_ms,
    signedDelayMedianMs: row.signed_delay_median_ms,
    feedCloses: row.feed_closes,
    feedErrors: row.feed_errors,
  };
}

function schemaSql() {
  return `
    CREATE TABLE IF NOT EXISTS probes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      at TEXT NOT NULL,
      measurement_started_at TEXT,
      measurement_started_mono_ms REAL,
      verdict TEXT NOT NULL,
      note TEXT,
      window_seconds INTEGER,
      handshake_ms INTEGER,
      subscribe_to_first_message_ms INTEGER,
      subscribe_to_first_trade_ms INTEGER,
      feed_messages INTEGER,
      feed_parse_failures INTEGER,
      measurement_feed_messages INTEGER,
      measurement_feed_parse_failures INTEGER,
      feed_parsed_trades INTEGER,
      feed_warmup_trades INTEGER,
      feed_sync_trades INTEGER,
      kraken_messages INTEGER,
      kraken_parse_failures INTEGER,
      measurement_kraken_parse_failures INTEGER,
      kraken_sync_trades INTEGER,
      sync_matched INTEGER,
      sync_coverage_pct INTEGER,
      kraken_trades INTEGER,
      our_trades INTEGER,
      reference_trades INTEGER,
      matched INTEGER,
      coverage_pct INTEGER,
      delay_median_ms INTEGER,
      delay_slow_ms INTEGER,
      delay_max_ms INTEGER,
      signed_delay_min_ms INTEGER,
      signed_delay_median_ms INTEGER,
      feed_closes INTEGER,
      feed_errors INTEGER
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
    CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);
    PRAGMA user_version = ${SCHEMA_VERSION};
  `;
}

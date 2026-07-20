# Trades feed heartbeat Telegram bot

This bot watches the app's trades feed server (`ws://feed.bitcoinstat.org:9000`). Once an hour it warms a feed subscription, opens Kraken's free public WebSocket, measures the same BTC/USD market for 90 seconds, then gives the app feed 10 seconds to deliver the last reference trades. If the feed stops relaying trades or starts lagging, the bot reports it to a Telegram chat in plain language.

## Why compare against Kraken

The feed server never acknowledges subscriptions and silently ignores unknown channels, so from the feed socket alone a quiet market and a dead server look identical. Kraken's own stream for the same pair is the ground truth: every trade Kraken shows should also arrive through our feed within a fraction of a second (measured healthy baseline: ~7 ms typical relay delay, 100% delivery).

## Verdicts

Each probe ends with one verdict:

- `ok` — the feed delivered the trades Kraken showed, with normal delay.
- `degraded` — at least one reference trade was missing, the slowest trades took more than 5 seconds, or the feed dropped the socket mid-probe.
- `down` — could not connect, or Kraken showed trades and the feed delivered none.
- `inconclusive` — Kraken produced no trades in the measurement window, was unreachable, or its connection broke during the probe.

A first `down`/`degraded` probe is re-checked once after `CONFIRM_DELAY_SECONDS` before the chat is alerted, so a one-off network hiccup does not page anyone.

If the Kraken reference is unavailable or disconnects, the bot records the reason and retries once after `CONFIRM_DELAY_SECONDS`. If the retry also fails, it warns the chat outside quiet hours that BitcoinTicker could not be checked; it never labels the app feed `down` from an incomplete reference.

Outside quiet hours, every `down`/`degraded` result sends a notification. An `ok` result sends a notification only when the last notified state was a problem (broke → recovered). Every Kraken trade must have a one-to-one app-feed match by exact price + quantity + side, bounded in both exchange time and monotonic receive time. Feed `time` is only a broad bound because its one-second server-side value can cross the Kraken exchange-second boundary. Within each class a maximum-cardinality, minimum-time-distance matcher assigns copies globally.

The comparison has explicit monotonic-clock boundaries: the app feed is subscribed first and warmed for 3 seconds, Kraken must acknowledge its subscription, then both active subscriptions are recorded for another 3-second overlapping sync. Nothing is cleared at the measurement boundary. A 2-second feed pre-roll protects copies that beat this process's direct Kraken socket. Both sockets remain recorded through the 10-second drain; sync and post-window Kraken trades are matching context only and never enter the verdict. This prevents equal-value trades outside the reference window from backfilling an in-window loss.

Exchange timestamps and local receive timestamps are stored separately. Relative delivery delay remains `feed receive − direct Kraken receive`, measured on a monotonic clock; its signed value is retained so genuine feed leads are visible. Raw message counts, parse failures, first-message/trade activation timings, all parsed feed trades, and overlapping-sync coverage are recorded for diagnosis.

## Quiet hours

Probes run around the clock and every result is recorded — quiet hours (23:00–09:00 Kyiv by default) only mute notifications. If the verdict changed overnight, the first probe after quiet hours end sends the catch-up message. Telegram commands work during quiet hours.

## Telegram commands

Restricted to `TELEGRAM_CHAT_ID` (this bot lives in its own group, separate from the app_store bot):

- `/stats` — current state of the feed, last probe, whether quiet hours are active, uptime.
- `/day` — the last 24 hours as a list: one line per probe with its persistent number (`№47 19:42 🟠 губилися угоди (дійшло 78%)`), plus totals and typical/worst delay.
- `/check` — run a probe right now and reply with the result (normally under two minutes including setup and delivery drain). While any probe is running, repeated `/check` commands are not processed: the first duplicate gets a short "already running" notice, the rest are dropped, and the scheduled hourly probe also never overlaps a manual one.
- `/details` — full breakdown of the last probe: counts, delays, handshake/activation time, overlapping-sync coverage, parse failures, and every reference trade with exchange-side time, price, quantity, side, and delivery status. Loss runs are described from the full ordered stream, so a delivered trade breaks a run and Kraken packet batching cannot invent one. `/details 47` (also `№47`) shows the same for a recorded probe number.

## Read-only HTTP API

Telegram commands answer fixed questions; the API exists for ad-hoc ones ("which hours degrade", "is coverage getting worse over weeks") without adding a command each time. It starts only when `API_TOKEN` is set and a port is available (`PORT` on Railway, `API_PORT` locally), and it never writes: queries run on a separate SQLite connection opened read-only.

Authenticate with `Authorization: Bearer $API_TOKEN` or `?token=$API_TOKEN`.

- `GET /health` — no auth, for Railway's healthcheck: uptime plus the last probe's time and verdict.
- `GET /api/stats` — current state, quiet hours, next probe, verdict totals over all history, last probe.
- `GET /api/probes?hours=24&limit=100&verdict=degraded,down` — probe rows, newest first. `since=<ISO>` instead of `hours`; `verdict` takes a comma-separated list.
- `GET /api/probes/<№>` — one probe with measurement, sync and drain Kraken context plus every parsed feed trade, including unmatched records and their collection phase. Same numbering as `/day` and `/details`.
- `GET /api/sql?q=SELECT...` (or the query as a POST body) — any single `SELECT`/`WITH` over `probes`, `trades`, `feed_trades`, `message_events`, or `kv`. Anything else is rejected, and at most 5000 rows come back.

```sh
curl -s -H "Authorization: Bearer $API_TOKEN" \
  "$BASE/api/sql?q=$(printf %s "SELECT substr(at,12,2) AS hour, COUNT(*) n, AVG(coverage_pct) cov FROM probes GROUP BY hour ORDER BY cov" | jq -sRr @uri)"
```

The token is the only thing protecting the history, so treat it like the bot token: set it in Railway Variables, never commit it.

## Local setup

```sh
cd trades_heartbeat
cp .env.example .env
```

Fill in `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Everything else has working defaults.

Run one probe without Telegram (messages print to the console):

```sh
npm run dry
```

Run one probe with real Telegram delivery:

```sh
npm run once
```

Run continuously:

```sh
npm start
```

No dependencies — the bot uses Node's built-in `WebSocket`, `fetch`, and `node:sqlite` (Node ≥ 22.13).

## Storage

SQLite via Node's built-in `node:sqlite` (needs Node ≥ 22.13; still zero npm dependencies). The database `heartbeat.db` has five tables:

- `probes` — one row per probe with verdict and connection/activation/parser/sync telemetry.
- `trades` — measurement reference trades plus sync/drain Kraken context, with exchange and receive times and the selected feed copy.
- `feed_trades` — every successfully parsed feed trade, including unmatched warmup and drain records.
- `message_events` — every feed/Kraken WebSocket message with source, phase, receive time, parsed-trade count and parse failures; a short raw preview is retained only for malformed messages.
- `kv` — service state: Telegram update offset, last notified verdict, first-run marker.

This is an internal diagnostic bot and its early history is disposable. A schema-version change recreates `probes`, `trades`, `feed_trades`, and `message_events` instead of running migrations; `kv` survives so Telegram updates are not replayed. Within one schema version history remains queryable with plain SQL.

For Railway, mount a volume; if `STATE_DIR` is not set, the bot automatically uses Railway's `RAILWAY_VOLUME_MOUNT_PATH`. `DB_FILE` overrides the exact path.

## Railway

Same pattern as the app_store bot: set the variables from `.env.example` in Railway Variables, mount a volume for state. No secrets beyond the Telegram token.

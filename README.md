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

Outside quiet hours, every `down`/`degraded` result sends a notification. An `ok` result sends a notification only when the last notified state was a problem (broke → recovered). Every Kraken trade must have a one-to-one price + quantity match from the app feed; feed timestamps are not used because they have one-second resolution.

The comparison has explicit boundaries: the app feed is subscribed first and warmed for 3 seconds, Kraken must acknowledge its subscription, then both live subscriptions stabilize for another 3 seconds before both buffers start empty at the same instant. After the 90-second reference window only the app feed remains open for a 10-second delivery drain. With startup and end races removed, even one Kraken trade is enough for a verdict; zero trades means there was nothing to test.

## Quiet hours

Probes run around the clock and every result is recorded — quiet hours (23:00–09:00 Kyiv by default) only mute notifications. If the verdict changed overnight, the first probe after quiet hours end sends the catch-up message. Telegram commands work during quiet hours.

## Telegram commands

Restricted to `TELEGRAM_CHAT_ID` (this bot lives in its own group, separate from the app_store bot):

- `/stats` — current state of the feed, last probe, whether quiet hours are active, uptime.
- `/day` — the last 24 hours as a list: one line per probe with its persistent number (`№47 19:42 🟠 губилися угоди (дійшло 78%)`), plus totals and typical/worst delay.
- `/check` — run a probe right now and reply with the result (normally under two minutes including setup and delivery drain). While any probe is running, repeated `/check` commands are not processed: the first duplicate gets a short "already running" notice, the rest are dropped, and the scheduled hourly probe also never overlaps a manual one.
- `/details` — full breakdown of the last probe: counts, delays, handshake time, and the list of lost trades (exchange-side time, price × quantity, up to 20 shown / 30 stored) with a burst-vs-scattered pattern hint for debugging the feed server. `/details 47` (also `№47`) shows the same for any probe number from `/day`; probe numbers are permanent.

## Read-only HTTP API

Telegram commands answer fixed questions; the API exists for ad-hoc ones ("which hours degrade", "is coverage getting worse over weeks") without adding a command each time. It starts only when `API_TOKEN` is set and a port is available (`PORT` on Railway, `API_PORT` locally), and it never writes: queries run on a separate SQLite connection opened read-only.

Authenticate with `Authorization: Bearer $API_TOKEN` or `?token=$API_TOKEN`.

- `GET /health` — no auth, for Railway's healthcheck: uptime plus the last probe's time and verdict.
- `GET /api/stats` — current state, quiet hours, next probe, verdict totals over all history, last probe.
- `GET /api/probes?hours=24&limit=100&verdict=degraded,down` — probe rows, newest first. `since=<ISO>` instead of `hours`; `verdict` takes a comma-separated list.
- `GET /api/probes/<№>` — one probe with all its trades (delivered and lost). Same numbering as `/day` and `/details`.
- `GET /api/sql?q=SELECT...` (or the query as a POST body) — any single `SELECT`/`WITH` over `probes`, `trades`, `kv`. Anything else is rejected, and at most 5000 rows come back.

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

SQLite via Node's built-in `node:sqlite` (needs Node ≥ 22.13; still zero npm dependencies). The database `heartbeat.db` has three tables:

- `probes` — one row per probe; the row id is the permanent probe number shown in `/day` and accepted by `/details`.
- `trades` — every reference trade with its delivered/lost status, linked to its probe.
- `kv` — service state: Telegram update offset, last notified verdict, first-run marker.

History is never trimmed, so long-term patterns (which hours degrade, whether it worsens over weeks) stay queryable with plain SQL. On first run the bot imports an existing pre-SQLite `heartbeat_state.json` (preserving probe numbers) and renames it to `.imported`.

For Railway, mount a volume; if `STATE_DIR` is not set, the bot automatically uses Railway's `RAILWAY_VOLUME_MOUNT_PATH`. `DB_FILE` overrides the exact path.

## Railway

Same pattern as the app_store bot: set the variables from `.env.example` in Railway Variables, mount a volume for state. No secrets beyond the Telegram token.

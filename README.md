# Trades feed heartbeat Telegram bot

This bot watches the app's trades feed server (`ws://feed.bitcoinstat.org:9000`). Once an hour it opens one socket to the feed and one socket to Kraken's free public WebSocket, listens to the same market (BTC/USD on Kraken) for ~90 seconds, and compares what both sides delivered. If the feed stops relaying trades or starts lagging, the bot reports it to a Telegram chat in plain language.

## Why compare against Kraken

The feed server never acknowledges subscriptions and silently ignores unknown channels, so from the feed socket alone a quiet market and a dead server look identical. Kraken's own stream for the same pair is the ground truth: every trade Kraken shows should also arrive through our feed within a fraction of a second (measured healthy baseline: ~7 ms typical relay delay, 100% delivery).

## Verdicts

Each probe ends with one verdict:

- `ok` — the feed delivered the trades Kraken showed, with normal delay.
- `degraded` — less than 80% of trades arrived, or the slowest trades took more than 5 seconds, or the feed dropped the socket mid-probe.
- `down` — could not connect, or Kraken showed trades and the feed delivered none.
- `inconclusive` — the market was too quiet to judge (fewer than 3 reference trades even after extending the window), or Kraken itself was unreachable. Never triggers a notification.

A first `down`/`degraded` probe is re-checked once after `CONFIRM_DELAY_SECONDS` before the chat is alerted, so a one-off network hiccup does not page anyone.

Notifications are sent only when the verdict changes (worked → broke, broke → recovered). Trades are matched between the two feeds by price + quantity; feed timestamps are not used because they have one-second resolution.

## Quiet hours

Probes run around the clock and every result is recorded — quiet hours (23:00–09:00 Kyiv by default) only mute notifications. If the verdict changed overnight, the first probe after quiet hours end sends the catch-up message. Telegram commands work during quiet hours.

## Telegram commands

Restricted to `TELEGRAM_CHAT_ID` (this bot lives in its own group, separate from the app_store bot):

- `/stats` — current state of the feed, last probe, whether quiet hours are active, uptime.
- `/day` — the last 24 hours as a list: one line per probe with its persistent number (`№47 19:42 🟠 губилися угоди (дійшло 78%)`), plus totals and typical/worst delay.
- `/check` — run a probe right now and reply with the result (~90 seconds). While any probe is running, repeated `/check` commands are not processed: the first duplicate gets a short "already running" notice, the rest are dropped, and the scheduled hourly probe also never overlaps a manual one.
- `/details` — full breakdown of the last probe: counts, delays, handshake time, and the list of lost trades (exchange-side time, price × quantity, up to 20 shown / 30 stored) with a burst-vs-scattered pattern hint for debugging the feed server. `/details 47` (also `№47`) shows the same for any probe number from `/day`; probe numbers are permanent.

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
- `lost_trades` — one row per lost trade, linked to its probe.
- `kv` — service state: Telegram update offset, last notified verdict, first-run marker.

History is never trimmed, so long-term patterns (which hours degrade, whether it worsens over weeks) stay queryable with plain SQL. On first run the bot imports an existing pre-SQLite `heartbeat_state.json` (preserving probe numbers) and renames it to `.imported`.

For Railway, mount a volume; if `STATE_DIR` is not set, the bot automatically uses Railway's `RAILWAY_VOLUME_MOUNT_PATH`. `DB_FILE` overrides the exact path.

## Railway

Same pattern as the app_store bot: set the variables from `.env.example` in Railway Variables, mount a volume for state. No secrets beyond the Telegram token.

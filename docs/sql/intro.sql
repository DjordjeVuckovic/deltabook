-- ============================================================
-- ORDERBOOK QUERY LESSONS
-- schema: orderbook_snapshots(ts, symbol, last_update_id, bids, asks)
-- bids/asks: JSONB array of [price, qty] pairs, best level first
--   bids[0] = best bid (highest price)
--   asks[0] = best ask (lowest price)
-- ============================================================


-- ── LESSON 1: Basic reads ────────────────────────────────────────────────────
-- Always start here. Understand your data before doing anything clever.

-- 1a. Most recent 10 snapshots
SELECT ts, symbol, last_update_id
FROM orderbook_snapshots
ORDER BY ts DESC
LIMIT 10;

-- 1b. How many snapshots do we have per symbol?
SELECT symbol, COUNT(*) AS snapshots
FROM orderbook_snapshots
GROUP BY symbol
ORDER BY symbol;

-- 1c. Time range of collected data
SELECT symbol,
       MIN(ts) AS first_snapshot,
       MAX(ts) AS last_snapshot
FROM orderbook_snapshots
GROUP BY symbol;


-- ── LESSON 2: Extracting values from JSONB ───────────────────────────────────
-- bids and asks are JSONB arrays. You need to know how to reach into them.
--
-- Operators:
--   ->   returns JSONB      (use for further chaining)
--   ->>  returns TEXT       (use when you want the final value)
--
-- bids->0        = first element as JSONB  → ["67457.71", "2.80164"]
-- bids->0->0     = price as JSONB          → "67457.71"
-- bids->0->>0    = price as TEXT           → 67457.71
-- (bids->0->>0)::numeric  = price as number

-- 2a. Pull best bid and best ask out of each snapshot
SELECT ts,
       symbol,
       (bids -> 0 ->> 0)::numeric AS best_bid_price,
       (bids -> 0 ->> 1)::numeric AS best_bid_qty,
       (asks -> 0 ->> 0)::numeric AS best_ask_price,
       (asks -> 0 ->> 1)::numeric AS best_ask_qty
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
ORDER BY ts DESC
LIMIT 20;


-- ── LESSON 3: Spread and mid-price ──────────────────────────────────────────
-- Spread = best_ask - best_bid
-- This is the MM's gross revenue per round-trip. Wide spread = illiquid.
--
-- Mid-price = (best_bid + best_ask) / 2
-- The "fair value" estimate between buy and sell side.

-- 3a. Spread and mid over time
SELECT ts,
       symbol,
       (bids -> 0 ->> 0)::numeric                                    AS best_bid,
       (asks -> 0 ->> 0)::numeric                                    AS best_ask,
       (asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric       AS spread,
       ((bids -> 0 ->> 0)::numeric + (asks -> 0 ->> 0)::numeric) / 2 AS mid_price
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
ORDER BY ts DESC
LIMIT 50;

-- 3b. Spread statistics over the last hour
-- Are spreads widening? Narrowing? This tells you if market conditions changed.
SELECT symbol,
       AVG((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric)    AS avg_spread,
       MIN((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric)    AS min_spread,
       MAX((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric)    AS max_spread,
       STDDEV((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric) AS spread_stddev
FROM orderbook_snapshots
WHERE ts > NOW() - INTERVAL '2 hour'
GROUP BY symbol;

-- find when that max spread in the last N hours occurred
SELECT ts,
     (asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric AS spread,
     (bids -> 0 ->> 0)::numeric AS best_bid,
     (asks -> 0 ->> 0)::numeric AS best_ask
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
AND ts > NOW() - INTERVAL '2 hour'
AND (asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric > 1.0
ORDER BY spread DESC;

-- ── LESSON 4: time_bucket — TimescaleDB's GROUP BY time ──────────────────────
-- time_bucket() is like DATE_TRUNC but designed for time series.
-- It buckets rows into fixed-size windows: 1 min, 5 min, 1 hour, etc.
-- This is how you go from tick-level data to OHLC-style aggregates.

-- 4a. Average spread per 1-minute bucket
SELECT time_bucket('1 minute', ts)                                  AS bucket,
       symbol,
       AVG((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric) AS avg_spread,
       COUNT(*)                                                     AS snapshot_count
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
  AND ts > NOW() - INTERVAL '1 hour'
GROUP BY bucket, symbol
ORDER BY bucket DESC;

-- 4b. Mid-price OHLC (open/high/low/close) per 1-minute bucket
-- This reconstructs a price candle from LOB data — no trades needed.
SELECT time_bucket('1 minute', ts) AS bucket,
       symbol,
       FIRST(mid_price, ts)        AS open,
       MAX(mid_price)              AS high,
       MIN(mid_price)              AS low,
       LAST(mid_price, ts)         AS close
FROM (SELECT ts,
             symbol,
             ((bids -> 0 ->> 0)::numeric + (asks -> 0 ->> 0)::numeric) / 2 AS mid_price
      FROM orderbook_snapshots
      WHERE symbol = 'BTCUSDT'
        AND ts > NOW() - INTERVAL '1 hour') sub
GROUP BY bucket, symbol
ORDER BY bucket DESC;


-- ── LESSON 5: Book depth ─────────────────────────────────────────────────────
-- Depth = total liquidity available across price levels.
-- Thin depth = a large order will move price a lot (high market impact).
-- Deep depth = resilient to large orders.
--
-- jsonb_array_elements() unpacks the JSONB array into individual rows.
-- This lets you aggregate across all levels, not just the best.

-- 5a. Total bid and ask depth (sum of all quantities) per snapshot
SELECT ts,
       symbol,
       (SELECT SUM((level ->> '1')::numeric)
        FROM jsonb_array_elements(bids) AS level) AS total_bid_depth,
       (SELECT SUM((level ->> '1')::numeric)
        FROM jsonb_array_elements(asks) AS level) AS total_ask_depth
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
ORDER BY ts DESC
LIMIT 20;

-- 5b. Average depth per minute — is liquidity drying up?
SELECT time_bucket('1 minute', ts)                                                          AS bucket,
       symbol,
       AVG((SELECT SUM((level ->> '1')::numeric) FROM jsonb_array_elements(bids) AS level)) AS avg_bid_depth,
       AVG((SELECT SUM((level ->> '1')::numeric) FROM jsonb_array_elements(asks) AS level)) AS avg_ask_depth
FROM orderbook_snapshots
WHERE symbol = 'BTCUSDT'
  AND ts > NOW() - INTERVAL '1 hour'
GROUP BY bucket, symbol
ORDER BY bucket DESC;


-- ── LESSON 6: Anomaly detection ─────────────────────────────────────────────
-- Find snapshots where something looks wrong.
-- Wide spread = MM pulled quotes (vol spike, news, outage).
-- Thin depth  = liquidity disappeared (someone swept the book).

-- 6a. Snapshots where spread is more than 2x the hourly average
-- These are moments worth investigating.
WITH avg_spread AS (SELECT AVG((asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric) AS val
                    FROM orderbook_snapshots
                    WHERE symbol = 'BTCUSDT'
                      AND ts > NOW() - INTERVAL '5 hour')
SELECT ts,
       (asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric AS spread,
       avg_spread.val                                          AS hourly_avg
FROM orderbook_snapshots,
     avg_spread
WHERE symbol = 'BTCUSDT'
  AND ts > NOW() - INTERVAL '5 hour'
  AND (asks -> 0 ->> 0)::numeric - (bids -> 0 ->> 0)::numeric > avg_spread.val * 2
ORDER BY spread DESC;
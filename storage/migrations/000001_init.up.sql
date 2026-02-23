-- orderbook snapshots: one row per periodic snapshot of the top N bid/ask levels
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol         TEXT        NOT NULL,
    last_update_id BIGINT      NOT NULL,
    bids           JSONB       NOT NULL,
    asks           JSONB       NOT NULL
);

SELECT create_hypertable('orderbook_snapshots', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_orderbook_snapshots_symbol_ts
    ON orderbook_snapshots (symbol, ts DESC);
# Factory Dashboard Endpoint Plan

## Goal

Add a read-only endpoint to the existing Flask service:

- `GET /factory-dashboard`

The endpoint reads from the scanner's SQLite database, assembles the dashboard object from normalized tables at request time, and returns raw JSON with no envelope.

The Flask app and the scanner run on the same machine. The scanner writes to SQLite; this endpoint reads from it. WAL mode is already enabled by the scanner's migrations.

## Current Repo Constraints

- This repo is a Flask app with routing in `app.py` / `routes.py` and config in `config.py`.
- The existing SQLAlchemy setup is global and driven by `DATABASE_URI`.
- The scanner's SQLite file is a separate database outside this repo's ORM.

The new endpoint should not use the existing `Flask-SQLAlchemy` connection. Use Python `sqlite3` with a dedicated read-only connection.

## Confirmed Schema

The scanner's SQLite schema is finalized. All column names below are confirmed.

### `vaults`

| Column | Type | Notes |
|--------|------|-------|
| `address` | TEXT PK | Checksummed Ethereum address |
| `chain_id` | INTEGER | Always 1 for this dashboard |
| `name` | TEXT | Vault display name (nullable) |
| `symbol` | TEXT | Vault symbol, e.g. `yvCurve-...` (nullable) |
| `active` | INTEGER | 1 = active, 0 = inactive |

### `strategies`

| Column | Type | Notes |
|--------|------|-------|
| `address` | TEXT PK | Checksummed Ethereum address |
| `chain_id` | INTEGER | Always 1 |
| `vault_address` | TEXT | FK to `vaults.address` |
| `name` | TEXT | Strategy display name (nullable) |
| `active` | INTEGER | 1 = active, 0 = inactive |
| `auction_address` | TEXT | Latest resolved auction (nullable) |
| `auction_updated_at` | TEXT | ISO timestamp of last auction refresh |

### `tokens`

| Column | Type | Notes |
|--------|------|-------|
| `address` | TEXT PK | Checksummed Ethereum address |
| `chain_id` | INTEGER | Always 1 |
| `name` | TEXT | Token name (nullable) |
| `symbol` | TEXT | Token symbol (nullable) |
| `decimals` | INTEGER | Token decimals |
| `price_usd` | TEXT | Latest USD price as decimal string (nullable) |
| `price_source` | TEXT | Price provider identifier (nullable) |
| `logo_url` | TEXT | Validated logo image URL (nullable) |

### `strategy_token_balances_latest`

| Column | Type | Notes |
|--------|------|-------|
| `strategy_address` | TEXT | FK to `strategies.address` |
| `token_address` | TEXT | FK to `tokens.address` |
| `raw_balance` | TEXT | Raw uint256 balance as string |
| `normalized_balance` | TEXT | Human-readable balance as decimal string |
| `block_number` | INTEGER | Block at which balance was read |
| `scanned_at` | TEXT | ISO timestamp |

Primary key: `(strategy_address, token_address)`

### `scan_runs`

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | TEXT PK | UUID |
| `started_at` | TEXT | ISO timestamp |
| `finished_at` | TEXT | ISO timestamp (nullable) |
| `status` | TEXT | `completed` or `failed` |
| `strategies_seen` | INTEGER | |
| `pairs_seen` | INTEGER | |
| `pairs_succeeded` | INTEGER | |
| `pairs_failed` | INTEGER | |

### Join keys

```text
strategy_token_balances_latest.strategy_address -> strategies.address
strategy_token_balances_latest.token_address    -> tokens.address
strategies.vault_address                        -> vaults.address
```

## Response Shape

```json
{
  "generatedAt": "2026-03-10T18:18:06.400211Z",
  "latestScanAt": "2026-03-10T18:10:00Z",
  "summary": {
    "rowCount": 1234,
    "strategyCount": 250,
    "tokenCount": 80
  },
  "tokens": [
    {
      "tokenAddress": "0x...",
      "tokenSymbol": "CRV",
      "tokenName": "Curve DAO Token",
      "strategyCount": 120,
      "latestScanAt": "2026-03-10T18:10:00Z",
      "tokenPriceUsd": "0.246543",
      "logoUrl": "https://..."
    }
  ],
  "rows": [
    {
      "strategyAddress": "0x...",
      "strategyName": "Strategy Convex ...",
      "vaultAddress": "0x...",
      "vaultName": "Curve ...",
      "vaultSymbol": "yvCurve-...",
      "auctionAddress": "0x...",
      "active": true,
      "scannedAt": "2026-03-10T18:10:00Z",
      "balances": [
        {
          "tokenAddress": "0x...",
          "tokenSymbol": "CRV",
          "tokenName": "Curve DAO Token",
          "normalizedBalance": "123.45",
          "tokenPriceUsd": "0.246543",
          "tokenLogoUrl": "https://..."
        }
      ]
    }
  ]
}
```

Notes:

- `generatedAt` is set by this API at response time (UTC ISO 8601).
- `latestScanAt` is `MAX(strategy_token_balances_latest.scanned_at)`.
- `active` is `strategies.active` cast to boolean.
- `auctionAddress` is `strategies.auction_address` (null if no mapping).
- `tokenLogoUrl` is `tokens.logo_url` (null if validation failed or pending).
- `tokenPriceUsd` is `tokens.price_usd` (null if price unavailable).
- Return raw JSON only. No `success`/`data` wrapper.
- Return `200` with empty arrays if the DB has no balance rows.

## Configuration

Add to `.env`:

- `FACTORY_DASHBOARD_DB_PATH` — absolute path to the scanner's SQLite file (required).
- `FACTORY_DASHBOARD_BUSY_TIMEOUT_MS` — SQLite busy timeout in ms (default: `5000`).
- `FACTORY_DASHBOARD_CACHE_MAX_AGE_SECONDS` — `Cache-Control` max-age (default: `30`).

Missing `FACTORY_DASHBOARD_DB_PATH` is a startup misconfiguration; fail loudly.

## SQLite Connection

Open a short-lived read-only connection per request:

```python
conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
conn.execute("PRAGMA query_only = ON")
conn.row_factory = sqlite3.Row
```

WAL mode is owned by the scanner. On first request, log a warning if `PRAGMA journal_mode` is not `wal`.

## Query Plan

Three queries, assembled into one response.

### 1. Summary + latestScanAt

```sql
SELECT
    COUNT(DISTINCT stbl.strategy_address) AS strategy_count,
    COUNT(DISTINCT stbl.token_address)    AS token_count,
    MAX(stbl.scanned_at)                  AS latest_scan_at
FROM strategy_token_balances_latest stbl
```

`rowCount` is the number of grouped dashboard rows (computed after row assembly, not from this query).

### 2. Token catalog

```sql
SELECT
    t.address       AS token_address,
    t.symbol        AS token_symbol,
    t.name          AS token_name,
    t.price_usd     AS token_price_usd,
    t.logo_url      AS logo_url,
    COUNT(DISTINCT stbl.strategy_address) AS strategy_count,
    MAX(stbl.scanned_at) AS latest_scan_at
FROM strategy_token_balances_latest stbl
JOIN tokens t ON t.address = stbl.token_address
GROUP BY t.address
ORDER BY strategy_count DESC, t.symbol ASC
```

Only tokens with at least one balance row are included.

### 3. Detail rows

```sql
SELECT
    stbl.strategy_address,
    s.name           AS strategy_name,
    s.vault_address,
    v.name           AS vault_name,
    v.symbol         AS vault_symbol,
    s.auction_address,
    s.active,
    stbl.scanned_at,
    stbl.token_address,
    t.symbol         AS token_symbol,
    t.name           AS token_name,
    t.price_usd      AS token_price_usd,
    t.logo_url       AS token_logo_url,
    stbl.normalized_balance
FROM strategy_token_balances_latest stbl
JOIN strategies s ON s.address = stbl.strategy_address
JOIN vaults v     ON v.address = s.vault_address
JOIN tokens t     ON t.address = stbl.token_address
ORDER BY s.vault_address, stbl.strategy_address, t.symbol
```

Group in Python by `(strategy_address, vault_address)` to produce rows with nested `balances[]`.

## Routing

Register a root-level route in `app.py`:

```python
@app.route("/factory-dashboard")
def factory_dashboard():
    ...
```

Keep the handler thin — delegate to `services/factory_dashboard.py`.

Module split:

- `services/factory_dashboard.py` — connection helper, queries, assembly
- `config.py` — new env vars
- `app.py` — route registration

## Error Handling

| Condition | Response |
|-----------|----------|
| `FACTORY_DASHBOARD_DB_PATH` not set | `500` |
| DB file missing or unreadable | `500` |
| Required table or column missing | `500` |
| Lock contention beyond `busy_timeout` | `503` with logging |
| DB readable but no rows | `200` with empty arrays |
| Null `auction_address`, `logo_url`, or `price_usd` | Preserve as `null` in response |

## HTTP Headers

```text
Cache-Control: public, max-age=30, stale-while-revalidate=300
Content-Type: application/json
```

## Implementation Sequence

1. **Config** — add `FACTORY_DASHBOARD_DB_PATH` and optional settings to `config.py` and `.env`.
2. **Read service** — add `services/factory_dashboard.py` with connection helper, three query functions, and response assembly.
3. **Route** — register `GET /factory-dashboard` in `app.py`, wire to service, add cache headers.
4. **Verify** — test against the scanner's live SQLite file. Confirm response shape, null handling, and concurrent-read behavior.

## Non-Goals

- Storing a pre-rendered dashboard JSON blob in SQLite
- Reusing the existing global `Flask-SQLAlchemy` db for this endpoint
- Refactoring the app framework
- Writing to the scanner's SQLite file

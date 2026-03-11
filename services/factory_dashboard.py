import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import quote


logger = logging.getLogger(__name__)

SUMMARY_SQL = """
SELECT
    COUNT(DISTINCT stbl.strategy_address) AS strategy_count,
    COUNT(DISTINCT stbl.token_address)    AS token_count,
    MAX(stbl.scanned_at)                  AS latest_scan_at
FROM strategy_token_balances_latest stbl
"""

TOKEN_CATALOG_SQL = """
SELECT
    t.address AS token_address,
    t.symbol AS token_symbol,
    t.name AS token_name,
    t.price_usd AS token_price_usd,
    {logo_column} AS logo_url,
    COUNT(DISTINCT stbl.strategy_address) AS strategy_count,
    MAX(stbl.scanned_at) AS latest_scan_at
FROM strategy_token_balances_latest stbl
JOIN tokens t ON t.address = stbl.token_address
GROUP BY t.address
ORDER BY strategy_count DESC, t.symbol ASC
"""

DETAIL_ROWS_SQL = """
SELECT
    stbl.strategy_address,
    s.name AS strategy_name,
    s.vault_address,
    v.name AS vault_name,
    v.symbol AS vault_symbol,
    {auction_column} AS auction_address,
    s.active,
    stbl.scanned_at,
    stbl.token_address,
    t.symbol AS token_symbol,
    t.name AS token_name,
    t.price_usd AS token_price_usd,
    {logo_column} AS token_logo_url,
    stbl.normalized_balance
FROM strategy_token_balances_latest stbl
JOIN strategies s ON s.address = stbl.strategy_address
JOIN vaults v ON v.address = s.vault_address
JOIN tokens t ON t.address = stbl.token_address
ORDER BY s.vault_address, stbl.strategy_address, t.symbol
"""

KICKS_SQL = """
SELECT
    k.strategy_address,
    k.tx_hash,
    k.status,
    k.token_address,
    k.usd_value,
    k.created_at,
    t.symbol AS token_symbol
FROM kick_txs k
LEFT JOIN tokens t ON t.address = k.token_address
WHERE k.tx_hash IS NOT NULL AND k.tx_hash != ''
ORDER BY k.strategy_address, k.created_at DESC
"""


class FactoryDashboardError(Exception):
    def __init__(self, message, status_code=500):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class FactoryDashboardService:
    def __init__(self, db_path, busy_timeout_ms):
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms
        self._journal_mode_checked = False
        self._journal_mode_lock = Lock()

    def get_dashboard(self):
        with self._connect() as conn:
            self._warn_if_not_wal(conn)
            schema_features = self._get_schema_features(conn)
            summary_row = conn.execute(SUMMARY_SQL).fetchone()
            token_rows = conn.execute(self._build_token_catalog_sql(schema_features)).fetchall()
            detail_rows = conn.execute(self._build_detail_rows_sql(schema_features)).fetchall()
            kick_rows = conn.execute(KICKS_SQL).fetchall() if schema_features["kick_txs"] else []

        kicks_by_strategy = self._group_kicks(kick_rows)
        rows = self._assemble_rows(detail_rows, kicks_by_strategy)
        latest_scan_at = summary_row["latest_scan_at"] if summary_row else None

        return {
            "generatedAt": self._utc_now(),
            "latestScanAt": latest_scan_at,
            "summary": {
                "rowCount": len(rows),
                "strategyCount": summary_row["strategy_count"] if summary_row else 0,
                "tokenCount": summary_row["token_count"] if summary_row else 0,
            },
            "tokens": [self._serialize_token_row(row) for row in token_rows],
            "rows": rows,
        }

    @contextmanager
    def _connect(self):
        db_path = Path(self.db_path)
        if not db_path.is_file():
            raise FactoryDashboardError("Factory dashboard database file is missing or unreadable")

        db_uri = f"file:{quote(db_path.as_posix(), safe='/')}?mode=ro"

        try:
            conn = sqlite3.connect(db_uri, uri=True)
            conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            conn.execute("PRAGMA query_only = ON")
            conn.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            logger.error("Failed to open factory dashboard database", exc_info=True)
            raise self._translate_sqlite_error(exc) from exc

        try:
            yield conn
        except sqlite3.Error as exc:
            logger.error("Factory dashboard query failed", exc_info=True)
            raise self._translate_sqlite_error(exc) from exc
        finally:
            conn.close()

    def _warn_if_not_wal(self, conn):
        if self._journal_mode_checked:
            return

        with self._journal_mode_lock:
            if self._journal_mode_checked:
                return

            try:
                journal_mode_row = conn.execute("PRAGMA journal_mode").fetchone()
                journal_mode = journal_mode_row[0] if journal_mode_row else None
                if str(journal_mode).lower() != "wal":
                    logger.warning("Factory dashboard SQLite journal mode is %s, expected wal", journal_mode)
            except sqlite3.Error:
                logger.warning("Unable to verify factory dashboard SQLite journal mode", exc_info=True)
            finally:
                self._journal_mode_checked = True

    def _serialize_token_row(self, row):
        return {
            "tokenAddress": row["token_address"],
            "tokenSymbol": row["token_symbol"],
            "tokenName": row["token_name"],
            "strategyCount": row["strategy_count"],
            "latestScanAt": row["latest_scan_at"],
            "tokenPriceUsd": row["token_price_usd"],
            "logoUrl": row["logo_url"],
        }

    def _group_kicks(self, kick_rows):
        kicks_by_strategy = {}
        for row in kick_rows:
            addr = row["strategy_address"]
            if addr not in kicks_by_strategy:
                kicks_by_strategy[addr] = []
            kicks = kicks_by_strategy[addr]
            if len(kicks) < 5:
                kicks.append({
                    "txHash": row["tx_hash"],
                    "status": row["status"],
                    "tokenSymbol": row["token_symbol"],
                    "usdValue": row["usd_value"],
                    "createdAt": row["created_at"],
                })
        return kicks_by_strategy

    def _assemble_rows(self, detail_rows, kicks_by_strategy):
        rows = []
        grouped_rows = {}

        for detail_row in detail_rows:
            row_key = (detail_row["strategy_address"], detail_row["vault_address"])
            grouped_row = grouped_rows.get(row_key)
            if grouped_row is None:
                grouped_row = {
                    "strategyAddress": detail_row["strategy_address"],
                    "strategyName": detail_row["strategy_name"],
                    "vaultAddress": detail_row["vault_address"],
                    "vaultName": detail_row["vault_name"],
                    "vaultSymbol": detail_row["vault_symbol"],
                    "auctionAddress": detail_row["auction_address"],
                    "active": bool(detail_row["active"]) if detail_row["active"] is not None else None,
                    "scannedAt": detail_row["scanned_at"],
                    "balances": [],
                    "kicks": kicks_by_strategy.get(detail_row["strategy_address"], []),
                }
                grouped_rows[row_key] = grouped_row
                rows.append(grouped_row)
            elif detail_row["scanned_at"] and (
                grouped_row["scannedAt"] is None or detail_row["scanned_at"] > grouped_row["scannedAt"]
            ):
                grouped_row["scannedAt"] = detail_row["scanned_at"]

            grouped_row["balances"].append(
                {
                    "tokenAddress": detail_row["token_address"],
                    "tokenSymbol": detail_row["token_symbol"],
                    "tokenName": detail_row["token_name"],
                    "normalizedBalance": detail_row["normalized_balance"],
                    "tokenPriceUsd": detail_row["token_price_usd"],
                    "tokenLogoUrl": detail_row["token_logo_url"],
                }
            )

        return rows

    def _get_schema_features(self, conn):
        return {
            "strategies.auction_address": self._has_column(conn, "strategies", "auction_address"),
            "tokens.logo_url": self._has_column(conn, "tokens", "logo_url"),
            "kick_txs": self._has_table(conn, "kick_txs"),
        }

    def _has_table(self, conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        ).fetchone()
        return row is not None

    def _has_column(self, conn, table_name, column_name):
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(row["name"] == column_name for row in rows)

    def _build_token_catalog_sql(self, schema_features):
        logo_column = "t.logo_url" if schema_features["tokens.logo_url"] else "NULL"
        return TOKEN_CATALOG_SQL.format(logo_column=logo_column)

    def _build_detail_rows_sql(self, schema_features):
        auction_column = "s.auction_address" if schema_features["strategies.auction_address"] else "NULL"
        logo_column = "t.logo_url" if schema_features["tokens.logo_url"] else "NULL"
        return DETAIL_ROWS_SQL.format(auction_column=auction_column, logo_column=logo_column)

    def _translate_sqlite_error(self, exc):
        message = str(exc).lower()
        if "locked" in message or "busy" in message:
            return FactoryDashboardError("Factory dashboard database is busy", status_code=503)
        if "no such table" in message or "no such column" in message:
            return FactoryDashboardError("Factory dashboard database schema is missing required tables or columns")
        return FactoryDashboardError("Factory dashboard query failed")

    @staticmethod
    def _utc_now():
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

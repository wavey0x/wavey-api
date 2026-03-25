import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import quote


logger = logging.getLogger(__name__)

STRATEGY_DETAIL_ROWS_SQL = """
SELECT
    'strategy' AS source_type,
    stbl.strategy_address AS source_address,
    s.name AS source_name,
    'vault' AS context_type,
    s.vault_address AS context_address,
    v.name AS context_name,
    v.symbol AS context_symbol,
    stbl.strategy_address AS strategy_address,
    s.name AS strategy_name,
    s.vault_address AS vault_address,
    v.name AS vault_name,
    v.symbol AS vault_symbol,
    {auction_column} AS auction_address,
    {auction_version_column} AS auction_version,
    {strategy_want_column} AS want_address,
    {strategy_want_symbol_column} AS want_symbol,
    {deposit_limit_column} AS deposit_limit,
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
{strategy_want_join}
ORDER BY s.vault_address, stbl.strategy_address, t.symbol
"""

FEE_BURNER_DETAIL_ROWS_SQL = """
SELECT
    'fee_burner' AS source_type,
    fbtbl.fee_burner_address AS source_address,
    fb.name AS source_name,
    NULL AS context_type,
    NULL AS context_address,
    NULL AS context_name,
    NULL AS context_symbol,
    NULL AS strategy_address,
    NULL AS strategy_name,
    NULL AS vault_address,
    NULL AS vault_name,
    NULL AS vault_symbol,
    {fee_burner_auction_column} AS auction_address,
    {fee_burner_auction_version_column} AS auction_version,
    {fee_burner_want_column} AS want_address,
    {fee_burner_want_symbol_column} AS want_symbol,
    NULL AS deposit_limit,
    1 AS active,
    fbtbl.scanned_at,
    fbtbl.token_address,
    t.symbol AS token_symbol,
    t.name AS token_name,
    t.price_usd AS token_price_usd,
    {logo_column} AS token_logo_url,
    fbtbl.normalized_balance
FROM fee_burner_token_balances_latest fbtbl
JOIN fee_burners fb ON fb.address = fbtbl.fee_burner_address
JOIN tokens t ON t.address = fbtbl.token_address
{fee_burner_want_join}
ORDER BY fbtbl.fee_burner_address, t.symbol
"""

KICKS_SQL_TEMPLATE = """
SELECT
    {source_type_expr} AS source_type,
    {source_address_expr} AS source_address,
    k.strategy_address,
    k.tx_hash,
    k.status,
    k.token_address,
    {kick_token_symbol_column} AS token_symbol,
    k.usd_value,
    k.created_at
FROM kick_txs k
LEFT JOIN tokens t ON t.address = k.token_address
WHERE k.tx_hash IS NOT NULL AND k.tx_hash != ''
ORDER BY {source_address_expr}, k.created_at DESC
"""

KICKS_DETAIL_SQL_TEMPLATE = """
SELECT
    k.id,
    k.run_id,
    {source_type_expr} AS source_type,
    {source_address_expr} AS source_address,
    {source_name_column} AS source_name,
    k.strategy_address,
    s.name AS strategy_name,
    k.token_address,
    {kick_token_symbol_column} AS token_symbol,
    k.auction_address,
    k.want_address,
    {kick_want_symbol_column} AS want_symbol,
    k.normalized_balance,
    k.sell_amount,
    k.starting_price,
    k.minimum_price,
    k.start_price_buffer_bps,
    k.min_price_buffer_bps,
    k.quote_amount,
    k.quote_response_json,
    k.price_usd,
    k.usd_value,
    k.status,
    k.tx_hash,
    k.gas_used,
    k.gas_price_gwei,
    k.block_number,
    k.error_message,
    k.created_at
FROM kick_txs k
LEFT JOIN strategies s ON s.address = {source_address_expr}
{fee_burner_join}
LEFT JOIN tokens t ON t.address = k.token_address
{want_token_join}
{status_clause}
ORDER BY k.created_at DESC
LIMIT ? OFFSET ?
"""

KICKS_COUNT_SQL = "SELECT COUNT(*) AS total FROM kick_txs"
KICKS_COUNT_FILTERED_SQL = "SELECT COUNT(*) AS total FROM kick_txs WHERE status = ?"


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
            detail_rows = conn.execute(self._build_strategy_detail_rows_sql(schema_features)).fetchall()
            if schema_features["fee_burner_rows"]:
                detail_rows.extend(conn.execute(self._build_fee_burner_detail_rows_sql(schema_features)).fetchall())
            kick_rows = conn.execute(self._build_kicks_sql(schema_features)).fetchall() if schema_features["kick_txs"] else []

        kicks_by_source = self._group_kicks(kick_rows)
        rows = self._assemble_rows(detail_rows, kicks_by_source)
        token_rows = self._build_token_catalog(detail_rows)
        latest_scan_at = max((row["scanned_at"] for row in detail_rows if row["scanned_at"]), default=None)
        summary = self._build_summary(rows, token_rows, latest_scan_at)

        return {
            "generatedAt": self._utc_now(),
            "latestScanAt": latest_scan_at,
            "summary": summary,
            "tokens": token_rows,
            "rows": rows,
        }

    def get_kicks(self, limit=100, offset=0, status=None):
        limit = min(max(int(limit), 1), 500)
        offset = max(int(offset), 0)

        with self._connect() as conn:
            self._warn_if_not_wal(conn)

            if not self._has_table(conn, "kick_txs"):
                return {"kicks": [], "total": 0}

            if status:
                count_row = conn.execute(KICKS_COUNT_FILTERED_SQL, (status,)).fetchone()
                detail_sql = self._build_kicks_detail_sql(self._get_schema_features(conn), include_status_filter=True)
                kick_rows = conn.execute(detail_sql, (status, limit, offset)).fetchall()
            else:
                count_row = conn.execute(KICKS_COUNT_SQL).fetchone()
                kick_rows = conn.execute(self._build_kicks_detail_sql(self._get_schema_features(conn)), (limit, offset)).fetchall()

        total = count_row["total"] if count_row else 0

        kicks = []
        for row in kick_rows:
            kick = {
                "id": row["id"],
                "runId": row["run_id"],
                "sourceType": row["source_type"],
                "sourceAddress": row["source_address"],
                "sourceName": row["source_name"],
                "strategyAddress": row["strategy_address"],
                "strategyName": row["strategy_name"],
                "tokenAddress": row["token_address"],
                "tokenSymbol": row["token_symbol"],
                "auctionAddress": row["auction_address"],
                "wantAddress": row["want_address"],
                "wantSymbol": row["want_symbol"],
                "normalizedBalance": row["normalized_balance"],
                "sellAmount": row["sell_amount"],
                "startingPrice": row["starting_price"],
                "minimumPrice": row["minimum_price"],
                "startPriceBufferBps": row["start_price_buffer_bps"],
                "minPriceBufferBps": row["min_price_buffer_bps"],
                "quoteAmount": row["quote_amount"],
                "quoteResponseJson": row["quote_response_json"],
                "priceUsd": row["price_usd"],
                "usdValue": row["usd_value"],
                "status": row["status"],
                "txHash": row["tx_hash"],
                "gasUsed": row["gas_used"],
                "gasPriceGwei": row["gas_price_gwei"],
                "blockNumber": row["block_number"],
                "errorMessage": row["error_message"],
                "createdAt": row["created_at"],
            }
            kicks.append(kick)

        return {"kicks": kicks, "total": total}

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

    def _group_kicks(self, kick_rows):
        kicks_by_source = {}
        for row in kick_rows:
            source_address = row["source_address"] or row["strategy_address"]
            if not source_address:
                continue
            source_key = (row["source_type"], source_address)
            if source_key not in kicks_by_source:
                kicks_by_source[source_key] = []
            kicks = kicks_by_source[source_key]
            if len(kicks) < 5:
                kicks.append({
                    "txHash": row["tx_hash"],
                    "status": row["status"],
                    "tokenSymbol": row["token_symbol"],
                    "usdValue": row["usd_value"],
                    "createdAt": row["created_at"],
                })
        return kicks_by_source

    def _assemble_rows(self, detail_rows, kicks_by_source):
        rows = []
        grouped_rows = {}

        for detail_row in detail_rows:
            source_key = (detail_row["source_type"], detail_row["source_address"])
            row_key = (detail_row["source_type"], detail_row["source_address"], detail_row["context_address"])
            grouped_row = grouped_rows.get(row_key)
            if grouped_row is None:
                grouped_row = {
                    "sourceType": detail_row["source_type"],
                    "sourceAddress": detail_row["source_address"],
                    "sourceName": detail_row["source_name"],
                    "contextType": detail_row["context_type"],
                    "contextAddress": detail_row["context_address"],
                    "contextName": detail_row["context_name"],
                    "contextSymbol": detail_row["context_symbol"],
                    "strategyAddress": detail_row["strategy_address"],
                    "strategyName": detail_row["strategy_name"],
                    "vaultAddress": detail_row["vault_address"],
                    "vaultName": detail_row["vault_name"],
                    "vaultSymbol": detail_row["vault_symbol"],
                    "auctionAddress": detail_row["auction_address"],
                    "auctionVersion": detail_row["auction_version"],
                    "wantAddress": detail_row["want_address"],
                    "wantSymbol": detail_row["want_symbol"],
                    "depositLimit": detail_row["deposit_limit"],
                    "active": bool(detail_row["active"]) if detail_row["active"] is not None else None,
                    "scannedAt": detail_row["scanned_at"],
                    "balances": [],
                    "kicks": kicks_by_source.get(source_key, []),
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

    def _build_token_catalog(self, detail_rows):
        tokens_by_address = {}

        for row in detail_rows:
            token_address = row["token_address"]
            if token_address not in tokens_by_address:
                tokens_by_address[token_address] = {
                    "tokenAddress": token_address,
                    "tokenSymbol": row["token_symbol"],
                    "tokenName": row["token_name"],
                    "tokenPriceUsd": row["token_price_usd"],
                    "logoUrl": row["token_logo_url"],
                    "latestScanAt": row["scanned_at"],
                    "strategyCount": 0,
                    "sourceCount": 0,
                    "_source_keys": set(),
                }

            token_row = tokens_by_address[token_address]
            token_row["tokenSymbol"] = token_row["tokenSymbol"] or row["token_symbol"]
            token_row["tokenName"] = token_row["tokenName"] or row["token_name"]
            token_row["tokenPriceUsd"] = token_row["tokenPriceUsd"] or row["token_price_usd"]
            token_row["logoUrl"] = token_row["logoUrl"] or row["token_logo_url"]
            if row["scanned_at"] and (
                token_row["latestScanAt"] is None or row["scanned_at"] > token_row["latestScanAt"]
            ):
                token_row["latestScanAt"] = row["scanned_at"]

            source_key = (row["source_type"], row["source_address"])
            if source_key not in token_row["_source_keys"]:
                token_row["_source_keys"].add(source_key)
                token_row["sourceCount"] += 1
                if row["source_type"] == "strategy":
                    token_row["strategyCount"] += 1

        token_rows = list(tokens_by_address.values())
        for token_row in token_rows:
            token_row.pop("_source_keys", None)

        token_rows.sort(
            key=lambda row: (-row["strategyCount"], (row["tokenSymbol"] or "").upper(), row["tokenAddress"])
        )
        return token_rows

    def _build_summary(self, rows, token_rows, latest_scan_at):
        strategy_count = len({row["sourceAddress"] for row in rows if row["sourceType"] == "strategy"})
        fee_burner_count = len({row["sourceAddress"] for row in rows if row["sourceType"] == "fee_burner"})
        return {
            "rowCount": len(rows),
            "sourceCount": len(rows),
            "strategyCount": strategy_count,
            "feeBurnerCount": fee_burner_count,
            "tokenCount": len(token_rows),
            "latestScanAt": latest_scan_at,
        }

    def _get_schema_features(self, conn):
        return {
            "strategies.auction_address": self._has_column(conn, "strategies", "auction_address"),
            "strategies.auction_version": self._has_column(conn, "strategies", "auction_version"),
            "strategies.want_address": self._has_column(conn, "strategies", "want_address"),
            "tokens.logo_url": self._has_column(conn, "tokens", "logo_url"),
            "vaults.deposit_limit": self._has_column(conn, "vaults", "deposit_limit"),
            "kick_txs": self._has_table(conn, "kick_txs"),
            "kick_txs.source_type": self._has_column(conn, "kick_txs", "source_type"),
            "kick_txs.source_address": self._has_column(conn, "kick_txs", "source_address"),
            "kick_txs.token_symbol": self._has_column(conn, "kick_txs", "token_symbol"),
            "kick_txs.want_symbol": self._has_column(conn, "kick_txs", "want_symbol"),
            "fee_burners": self._has_table(conn, "fee_burners"),
            "fee_burners.auction_address": self._has_column(conn, "fee_burners", "auction_address"),
            "fee_burners.auction_version": self._has_column(conn, "fee_burners", "auction_version"),
            "fee_burners.want_address": self._has_column(conn, "fee_burners", "want_address"),
            "fee_burner_rows": self._has_table(conn, "fee_burners")
            and self._has_table(conn, "fee_burner_token_balances_latest"),
        }

    def _has_table(self, conn, table_name):
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        ).fetchone()
        return row is not None

    def _has_column(self, conn, table_name, column_name):
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(row["name"] == column_name for row in rows)

    def _build_strategy_detail_rows_sql(self, schema_features):
        auction_column = "s.auction_address" if schema_features["strategies.auction_address"] else "NULL"
        auction_version_column = "s.auction_version" if schema_features["strategies.auction_version"] else "NULL"
        logo_column = "t.logo_url" if schema_features["tokens.logo_url"] else "NULL"
        deposit_limit_column = "v.deposit_limit" if schema_features["vaults.deposit_limit"] else "NULL"
        if schema_features["strategies.want_address"]:
            strategy_want_column = "s.want_address"
            strategy_want_symbol_column = "wt.symbol"
            strategy_want_join = "LEFT JOIN tokens wt ON wt.address = s.want_address"
        else:
            strategy_want_column = "NULL"
            strategy_want_symbol_column = "NULL"
            strategy_want_join = ""

        return STRATEGY_DETAIL_ROWS_SQL.format(
            auction_column=auction_column,
            auction_version_column=auction_version_column,
            logo_column=logo_column,
            deposit_limit_column=deposit_limit_column,
            strategy_want_column=strategy_want_column,
            strategy_want_symbol_column=strategy_want_symbol_column,
            strategy_want_join=strategy_want_join,
        )

    def _build_fee_burner_detail_rows_sql(self, schema_features):
        logo_column = "t.logo_url" if schema_features["tokens.logo_url"] else "NULL"
        fee_burner_auction_column = "fb.auction_address" if schema_features["fee_burners.auction_address"] else "NULL"
        fee_burner_auction_version_column = (
            "fb.auction_version" if schema_features["fee_burners.auction_version"] else "NULL"
        )

        if schema_features["fee_burners.want_address"]:
            fee_burner_want_column = "fb.want_address"
            fee_burner_want_symbol_column = "wt.symbol"
            fee_burner_want_join = "LEFT JOIN tokens wt ON wt.address = fb.want_address"
        else:
            fee_burner_want_column = "NULL"
            fee_burner_want_symbol_column = "NULL"
            fee_burner_want_join = ""

        return FEE_BURNER_DETAIL_ROWS_SQL.format(
            fee_burner_auction_column=fee_burner_auction_column,
            fee_burner_auction_version_column=fee_burner_auction_version_column,
            fee_burner_want_column=fee_burner_want_column,
            fee_burner_want_symbol_column=fee_burner_want_symbol_column,
            fee_burner_want_join=fee_burner_want_join,
            logo_column=logo_column,
        )

    def _build_kick_source_expressions(self, schema_features):
        if schema_features["kick_txs.source_type"]:
            source_type_expr = "COALESCE(k.source_type, CASE WHEN k.strategy_address IS NOT NULL THEN 'strategy' END)"
        else:
            source_type_expr = "'strategy'"

        if schema_features["kick_txs.source_address"]:
            source_address_expr = "COALESCE(k.source_address, k.strategy_address)"
        else:
            source_address_expr = "k.strategy_address"

        return source_type_expr, source_address_expr

    def _build_kicks_sql(self, schema_features):
        source_type_expr, source_address_expr = self._build_kick_source_expressions(schema_features)
        kick_token_symbol_column = "COALESCE(k.token_symbol, t.symbol)" if schema_features["kick_txs.token_symbol"] else "t.symbol"
        return KICKS_SQL_TEMPLATE.format(
            source_type_expr=source_type_expr,
            source_address_expr=source_address_expr,
            kick_token_symbol_column=kick_token_symbol_column,
        )

    def _build_kicks_detail_sql(self, schema_features, include_status_filter=False):
        source_type_expr, source_address_expr = self._build_kick_source_expressions(schema_features)
        kick_token_symbol_column = "COALESCE(k.token_symbol, t.symbol)" if schema_features["kick_txs.token_symbol"] else "t.symbol"
        if schema_features["kick_txs.want_symbol"]:
            kick_want_symbol_column = "COALESCE(k.want_symbol, wt.symbol)"
        else:
            kick_want_symbol_column = "wt.symbol"

        if schema_features["fee_burners"]:
            fee_burner_join = f"LEFT JOIN fee_burners fb ON fb.address = {source_address_expr}"
            source_name_column = f"CASE WHEN {source_type_expr} = 'fee_burner' THEN fb.name ELSE s.name END"
        else:
            fee_burner_join = ""
            source_name_column = "s.name"

        want_token_join = "LEFT JOIN tokens wt ON wt.address = k.want_address"
        status_clause = "WHERE k.status = ?" if include_status_filter else ""

        return KICKS_DETAIL_SQL_TEMPLATE.format(
            source_type_expr=source_type_expr,
            source_address_expr=source_address_expr,
            source_name_column=source_name_column,
            fee_burner_join=fee_burner_join,
            kick_token_symbol_column=kick_token_symbol_column,
            kick_want_symbol_column=kick_want_symbol_column,
            want_token_join=want_token_join,
            status_clause=status_clause,
        )

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

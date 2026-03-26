import logging
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from threading import Lock
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from web3 import Web3
from hexbytes import HexBytes

from services.multicall import batch_calls
from services.web3_services import setup_web3


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
    {operation_type_expr} AS operation_type,
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
    {operation_type_expr} AS operation_type,
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
    {kick_auctionscan_round_id_column} AS auctionscan_round_id,
    {kick_auctionscan_last_checked_at_column} AS auctionscan_last_checked_at,
    {kick_auctionscan_matched_at_column} AS auctionscan_matched_at,
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

STRATEGY_DEPLOY_CONTEXT_SQL = """
SELECT
    s.address AS strategy_address,
    s.name AS strategy_name,
    s.auction_address AS auction_address,
    s.want_address AS want_address,
    wt.symbol AS want_symbol,
    s.active AS active,
    stbl.token_address AS token_address,
    stbl.raw_balance AS raw_balance,
    stbl.normalized_balance AS normalized_balance,
    t.symbol AS token_symbol,
    t.decimals AS token_decimals,
    t.price_usd AS token_price_usd
FROM strategies s
LEFT JOIN tokens wt ON wt.address = s.want_address
LEFT JOIN strategy_token_balances_latest stbl ON stbl.strategy_address = s.address
LEFT JOIN tokens t ON t.address = stbl.token_address
WHERE s.address = ?
ORDER BY t.symbol, stbl.token_address
"""

SINGLE_AUCTION_FACTORY_ABI = [
    {
        "inputs": [],
        "name": "getAllAuctions",
        "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "_want", "type": "address"},
            {"internalType": "address", "name": "_receiver", "type": "address"},
            {"internalType": "address", "name": "_governance", "type": "address"},
            {"internalType": "uint256", "name": "_startingPrice", "type": "uint256"},
            {"internalType": "bytes32", "name": "_salt", "type": "bytes32"},
        ],
        "name": "createNewAuction",
        "outputs": [{"internalType": "address", "name": "newAuction", "type": "address"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

AUCTION_IDENTITY_ABI = [
    {
        "inputs": [],
        "name": "governance",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "receiver",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "want",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]


class FactoryDashboardError(Exception):
    def __init__(self, message, status_code=500):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class FactoryDashboardService:
    def __init__(
        self,
        db_path,
        busy_timeout_ms,
        *,
        deploy_chain_id=1,
        deploy_factory_address="0xbA7FCb508c7195eE5AE823F37eE2c11D7ED52F8e",
        deploy_governance_address="0xb634316E06cC0B358437CbadD4dC94F1D3a92B3b",
        deploy_start_price_buffer_bps=1000,
        deploy_require_curve_quote=True,
        deploy_price_base_url="https://prices.wavey.info",
        deploy_price_api_key=None,
        deploy_price_timeout_seconds=10,
        auctionscan_base_url="https://auctionscan.info",
        auctionscan_api_base_url="https://auctionscan.info/api",
        auctionscan_recheck_seconds=90,
    ):
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms
        self._journal_mode_checked = False
        self._journal_mode_lock = Lock()
        self.deploy_chain_id = int(deploy_chain_id)
        self.deploy_factory_address = self._normalize_address(deploy_factory_address)
        self.deploy_governance_address = self._normalize_address(deploy_governance_address)
        self.deploy_start_price_buffer_bps = int(deploy_start_price_buffer_bps)
        self.deploy_require_curve_quote = bool(deploy_require_curve_quote)
        self.deploy_price_base_url = str(deploy_price_base_url).rstrip("/")
        self.deploy_price_api_key = deploy_price_api_key
        self.deploy_price_timeout_seconds = int(deploy_price_timeout_seconds)
        self.auctionscan_base_url = str(auctionscan_base_url).rstrip("/")
        self.auctionscan_api_base_url = str(auctionscan_api_base_url).rstrip("/")
        self.auctionscan_recheck_seconds = max(int(auctionscan_recheck_seconds), 0)
        self.web3 = setup_web3()

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
                "operationType": row["operation_type"] or "kick",
                "sourceType": row["source_type"],
                "sourceAddress": self._optional_normalize_address(row["source_address"]),
                "sourceName": row["source_name"],
                "strategyAddress": self._optional_normalize_address(row["strategy_address"]),
                "strategyName": row["strategy_name"],
                "tokenAddress": self._optional_normalize_address(row["token_address"]),
                "tokenSymbol": row["token_symbol"],
                "auctionAddress": self._optional_normalize_address(row["auction_address"]),
                "wantAddress": self._optional_normalize_address(row["want_address"]),
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
                "chainId": self.deploy_chain_id,
                "auctionScanRoundId": row["auctionscan_round_id"],
                "auctionScanLastCheckedAt": row["auctionscan_last_checked_at"],
                "auctionScanMatchedAt": row["auctionscan_matched_at"],
                "auctionScanAuctionUrl": self._build_auctionscan_auction_url(row["auction_address"]),
                "auctionScanRoundUrl": self._build_auctionscan_round_url(
                    row["auction_address"],
                    row["auctionscan_round_id"],
                ),
                "errorMessage": row["error_message"],
                "createdAt": row["created_at"],
            }
            kicks.append(kick)

        return {"kicks": kicks, "total": total}

    def resolve_kick_auctionscan(self, kick_id):
        with self._connect() as conn:
            self._warn_if_not_wal(conn)
            schema_features = self._get_schema_features(conn)
            kick = self._load_kick_auctionscan_context(conn, kick_id, schema_features)

        if kick["auctionscan_round_id"] is not None:
            return self._build_kick_auctionscan_response(
                kick,
                resolved=True,
                cached=True,
            )

        if not kick["eligible"]:
            return self._build_kick_auctionscan_response(
                kick,
                resolved=False,
                cached=False,
            )

        if self._should_skip_auctionscan_recheck(kick["auctionscan_last_checked_at"]):
            return self._build_kick_auctionscan_response(
                kick,
                resolved=False,
                cached=False,
            )

        round_payload = self._lookup_auctionscan_round(
            auction_address=kick["auction_address"],
            from_token=kick["token_address"],
            transaction_hash=kick["tx_hash"],
        )
        checked_at = self._utc_now()

        if round_payload and round_payload.get("round_id") is not None:
            round_id = int(round_payload["round_id"])
            matched_at = checked_at
            self._persist_kick_auctionscan_match(
                kick_id,
                round_id=round_id,
                checked_at=checked_at,
                matched_at=matched_at,
            )
            kick["auctionscan_round_id"] = round_id
            kick["auctionscan_last_checked_at"] = checked_at
            kick["auctionscan_matched_at"] = matched_at
            return self._build_kick_auctionscan_response(
                kick,
                resolved=True,
                cached=False,
            )

        self._persist_kick_auctionscan_check(kick_id, checked_at=checked_at)
        kick["auctionscan_last_checked_at"] = checked_at
        return self._build_kick_auctionscan_response(
            kick,
            resolved=False,
            cached=False,
        )

    def build_strategy_deploy_tx(self, strategy_address):
        checksum_strategy = self._normalize_address(strategy_address)
        strategy_key = checksum_strategy.lower()

        with self._connect() as conn:
            self._warn_if_not_wal(conn)
            strategy_context = self._load_strategy_deploy_context(conn, strategy_key)

        if strategy_context["auctionAddress"]:
            raise FactoryDashboardError("Strategy already has an auction mapped", status_code=409)
        if not strategy_context["wantAddress"]:
            raise FactoryDashboardError("Strategy is missing want token metadata", status_code=409)

        balance = self._select_deploy_balance(strategy_context)
        quote = self._quote_token(
            token_in=balance["tokenAddress"],
            token_out=strategy_context["wantAddress"],
            amount_in=balance["rawBalance"],
        )
        starting_price = self._compute_starting_price(
            quote["amountOutRaw"],
            quote["tokenOutDecimals"],
        )

        if self.deploy_require_curve_quote and quote["providerAmounts"].get("curve", 0) <= 0:
            curve_status = quote["providerStatuses"].get("curve", "not present")
            raise FactoryDashboardError(
                f"Curve quote unavailable for deploy inference (status: {curve_status})",
                status_code=409,
            )

        matching_auctions = self._find_matching_auctions(
            want_address=strategy_context["wantAddress"],
            receiver_address=checksum_strategy,
            governance_address=self.deploy_governance_address,
        )
        if matching_auctions:
            raise FactoryDashboardError(
                f"Matching auction already exists in target factory: {matching_auctions[0]}",
                status_code=409,
            )

        salt = self._build_deploy_salt(
            strategy_address=checksum_strategy,
            want_address=strategy_context["wantAddress"],
        )
        predicted_address, tx_data = self._build_create_auction_tx(
            want_address=strategy_context["wantAddress"],
            receiver_address=checksum_strategy,
            governance_address=self.deploy_governance_address,
            starting_price=starting_price,
            salt=salt,
        )

        return {
            "strategyAddress": checksum_strategy,
            "strategyName": strategy_context["strategyName"],
            "factoryAddress": self.deploy_factory_address,
            "governanceAddress": self.deploy_governance_address,
            "receiverAddress": checksum_strategy,
            "wantAddress": strategy_context["wantAddress"],
            "wantSymbol": strategy_context["wantSymbol"],
            "startingPrice": str(starting_price),
            "startPriceBufferBps": self.deploy_start_price_buffer_bps,
            "predictedAuctionAddress": predicted_address,
            "salt": salt,
            "inference": {
                "sellTokenAddress": balance["tokenAddress"],
                "sellTokenSymbol": balance["tokenSymbol"],
                "rawBalance": balance["rawBalance"],
                "normalizedBalance": balance["normalizedBalance"],
                "priceUsd": balance["priceUsd"],
                "usdValue": balance["usdValue"],
                "quoteAmountOutRaw": str(quote["amountOutRaw"]),
                "quoteRequestUrl": quote["requestUrl"],
                "providerStatuses": quote["providerStatuses"],
            },
            "txRequest": {
                "to": self.deploy_factory_address,
                "data": tx_data,
                "value": "0x0",
                "chainId": self.deploy_chain_id,
            },
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

    @contextmanager
    def _connect_rw(self):
        db_path = Path(self.db_path)
        if not db_path.is_file():
            raise FactoryDashboardError("Factory dashboard database file is missing or unreadable")

        try:
            conn = sqlite3.connect(db_path)
            conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            conn.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            logger.error("Failed to open factory dashboard database for write", exc_info=True)
            raise self._translate_sqlite_error(exc) from exc

        try:
            yield conn
            conn.commit()
        except sqlite3.Error as exc:
            conn.rollback()
            logger.error("Factory dashboard write failed", exc_info=True)
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

    def _load_strategy_deploy_context(self, conn, strategy_address):
        rows = conn.execute(STRATEGY_DEPLOY_CONTEXT_SQL, (strategy_address,)).fetchall()
        if not rows:
            raise FactoryDashboardError("Strategy not found", status_code=404)

        first = rows[0]
        context = {
            "strategyAddress": self._normalize_address(first["strategy_address"]),
            "strategyName": first["strategy_name"],
            "auctionAddress": self._optional_normalize_address(first["auction_address"]),
            "wantAddress": self._optional_normalize_address(first["want_address"]),
            "wantSymbol": first["want_symbol"],
            "active": bool(first["active"]) if first["active"] is not None else None,
            "balances": [],
        }

        for row in rows:
            token_address = row["token_address"]
            if not token_address:
                continue
            context["balances"].append(
                {
                    "tokenAddress": self._normalize_address(token_address),
                    "rawBalance": row["raw_balance"],
                    "normalizedBalance": row["normalized_balance"],
                    "tokenSymbol": row["token_symbol"],
                    "tokenDecimals": row["token_decimals"],
                    "priceUsd": row["token_price_usd"],
                }
            )

        return context

    def _load_kick_auctionscan_context(self, conn, kick_id, schema_features):
        if not schema_features["kick_txs"]:
            raise FactoryDashboardError("Kick history is unavailable", status_code=404)

        round_id_column = "k.auctionscan_round_id" if schema_features["kick_txs.auctionscan_round_id"] else "NULL"
        last_checked_at_column = (
            "k.auctionscan_last_checked_at" if schema_features["kick_txs.auctionscan_last_checked_at"] else "NULL"
        )
        matched_at_column = "k.auctionscan_matched_at" if schema_features["kick_txs.auctionscan_matched_at"] else "NULL"

        row = conn.execute(
            f"""
            SELECT
                k.id,
                COALESCE(k.operation_type, 'kick') AS operation_type,
                k.status,
                k.tx_hash,
                k.auction_address,
                k.token_address,
                {round_id_column} AS auctionscan_round_id,
                {last_checked_at_column} AS auctionscan_last_checked_at,
                {matched_at_column} AS auctionscan_matched_at
            FROM kick_txs k
            WHERE k.id = ?
            """,
            (kick_id,),
        ).fetchone()
        if row is None:
            raise FactoryDashboardError("Kick not found", status_code=404)

        operation_type = row["operation_type"] or "kick"
        status = row["status"]
        auction_address = self._optional_normalize_address(row["auction_address"])
        token_address = self._optional_normalize_address(row["token_address"])
        tx_hash = row["tx_hash"]
        eligible = (
            operation_type == "kick"
            and status == "CONFIRMED"
            and auction_address is not None
            and token_address is not None
            and bool(tx_hash)
        )

        return {
            "id": row["id"],
            "operation_type": operation_type,
            "status": status,
            "tx_hash": tx_hash,
            "auction_address": auction_address,
            "token_address": token_address,
            "auctionscan_round_id": row["auctionscan_round_id"],
            "auctionscan_last_checked_at": row["auctionscan_last_checked_at"],
            "auctionscan_matched_at": row["auctionscan_matched_at"],
            "eligible": eligible,
        }

    def _select_deploy_balance(self, strategy_context):
        want_address = strategy_context["wantAddress"]
        candidates = []

        for balance in strategy_context["balances"]:
            token_address = balance["tokenAddress"]
            if token_address.lower() == want_address.lower():
                continue

            raw_balance = self._parse_decimal(balance["rawBalance"])
            normalized_balance = self._parse_decimal(balance["normalizedBalance"])
            price_usd = self._parse_decimal(balance["priceUsd"])
            if raw_balance is None or normalized_balance is None or price_usd is None:
                continue
            if raw_balance <= 0 or normalized_balance <= 0 or price_usd <= 0:
                continue

            usd_value = normalized_balance * price_usd
            candidates.append(
                {
                    **balance,
                    "usdValue": str(usd_value),
                }
            )

        candidates.sort(
            key=lambda item: (
                -Decimal(item["usdValue"]),
                item["tokenAddress"].lower(),
            )
        )

        if not candidates:
            raise FactoryDashboardError(
                "No eligible priced non-want token balance is available to infer deploy starting price",
                status_code=409,
            )

        return candidates[0]

    def _quote_token(self, *, token_in, token_out, amount_in):
        params = {
            "token_in": self._normalize_address(token_in),
            "token_out": self._normalize_address(token_out),
            "amount_in": str(amount_in),
            "chain_id": self.deploy_chain_id,
            "use_underlying": "true",
        }
        query_string = urlencode(params)
        request_url = f"{self.deploy_price_base_url}/v1/quote?{query_string}"
        headers = {"Accept": "application/json"}
        if self.deploy_price_api_key:
            headers["Authorization"] = f"Bearer {self.deploy_price_api_key}"

        last_result = None
        for attempt in range(2):
            payload = self._http_get_json(request_url, headers=headers)
            result = self._parse_quote_response(payload, request_url)
            last_result = result
            if result["amountOutRaw"] is not None:
                return result
            if attempt == 0 and result["providerStatuses"]:
                time.sleep(2.0)

        if last_result is None or last_result["amountOutRaw"] is None:
            raise FactoryDashboardError("No quote available to infer deploy starting price", status_code=409)
        return last_result

    def _parse_quote_response(self, payload, request_url):
        amount_out_raw = None
        token_out_decimals = None
        provider_statuses = {}
        provider_amounts = {}

        if isinstance(payload, dict):
            summary = payload.get("summary")
            if isinstance(summary, dict):
                high_amount_out = summary.get("high_amount_out")
                parsed = self._parse_decimal(high_amount_out)
                if parsed is not None:
                    amount_out_raw = int(parsed)

            token_out_data = payload.get("token_out")
            if isinstance(token_out_data, dict):
                raw_decimals = token_out_data.get("decimals")
                if raw_decimals is not None:
                    try:
                        token_out_decimals = int(raw_decimals)
                    except (TypeError, ValueError):
                        token_out_decimals = None

            providers = payload.get("providers")
            if isinstance(providers, dict):
                for name, entry in providers.items():
                    if not isinstance(entry, dict):
                        continue
                    provider_statuses[name] = entry.get("status")
                    parsed_amount = self._parse_decimal(entry.get("amount_out"))
                    if parsed_amount is not None:
                        provider_amounts[name] = int(parsed_amount)

        return {
            "amountOutRaw": amount_out_raw,
            "tokenOutDecimals": token_out_decimals,
            "providerStatuses": provider_statuses,
            "providerAmounts": provider_amounts,
            "requestUrl": request_url,
        }

    def _lookup_auctionscan_round(self, *, auction_address, from_token, transaction_hash):
        params = {
            "chain_id": self.deploy_chain_id,
            "from_token": self._normalize_address(from_token),
            "transaction_hash": str(transaction_hash),
        }
        request_url = (
            f"{self.auctionscan_api_base_url}/auctions/"
            f"{self._normalize_address(auction_address)}/rounds?{urlencode(params)}"
        )
        payload = self._http_get_json(
            request_url,
            headers={"Accept": "application/json"},
            timeout_seconds=self.deploy_price_timeout_seconds,
            error_context="AuctionScan request",
            not_found_returns_none=True,
        )
        if payload is None:
            return None
        if not isinstance(payload, dict):
            return None
        rounds = payload.get("rounds")
        if not isinstance(rounds, list) or not rounds:
            return None
        first = rounds[0]
        return first if isinstance(first, dict) else None

    def _compute_starting_price(self, amount_out_raw, token_out_decimals):
        parsed_amount = self._parse_decimal(amount_out_raw)
        if parsed_amount is None or parsed_amount <= 0:
            raise FactoryDashboardError("Quote amount is missing or zero", status_code=409)
        if token_out_decimals is None:
            raise FactoryDashboardError("Quote response is missing output token decimals", status_code=502)

        normalized = parsed_amount / (Decimal(10) ** int(token_out_decimals))
        buffer = Decimal(1) + Decimal(self.deploy_start_price_buffer_bps) / Decimal(10_000)
        starting_price = int((normalized * buffer).to_integral_value(rounding=ROUND_CEILING))
        if starting_price <= 0:
            raise FactoryDashboardError("Computed starting price is zero", status_code=409)
        return starting_price

    def _find_matching_auctions(self, *, want_address, receiver_address, governance_address):
        try:
            factory = self.web3.eth.contract(
                address=self.web3.to_checksum_address(self.deploy_factory_address),
                abi=SINGLE_AUCTION_FACTORY_ABI,
            )
            auction_addresses = factory.functions.getAllAuctions().call()
        except Exception as exc:
            raise FactoryDashboardError(f"Unable to read target auction factory: {exc}", status_code=502) from exc

        want_key = want_address.lower()
        receiver_key = receiver_address.lower()
        governance_key = governance_address.lower()
        matches = []

        try:
            calls = []
            keys = []
            for auction_address in auction_addresses:
                normalized_auction = self._normalize_address(auction_address)
                for field_name in ("want", "receiver", "governance"):
                    calls.append((normalized_auction, field_name, []))
                    keys.append((normalized_auction, field_name))

            results = batch_calls(self.web3, calls, abi=AUCTION_IDENTITY_ABI)
            metadata = {}
            for key, result in zip(keys, results):
                auction_address, field_name = key
                if result is None:
                    continue
                metadata.setdefault(auction_address, {})[field_name] = self._normalize_address(result).lower()

            for auction_address, fields in metadata.items():
                if (
                    fields.get("want") == want_key
                    and fields.get("receiver") == receiver_key
                    and fields.get("governance") == governance_key
                ):
                    matches.append(auction_address)
        except Exception:
            for auction_address in auction_addresses:
                try:
                    auction = self.web3.eth.contract(
                        address=self.web3.to_checksum_address(auction_address),
                        abi=AUCTION_IDENTITY_ABI,
                    )
                    want = self._normalize_address(auction.functions.want().call()).lower()
                    receiver = self._normalize_address(auction.functions.receiver().call()).lower()
                    governance = self._normalize_address(auction.functions.governance().call()).lower()
                except Exception:
                    continue

                if want == want_key and receiver == receiver_key and governance == governance_key:
                    matches.append(self._normalize_address(auction_address))

        return matches

    def _build_deploy_salt(self, *, strategy_address, want_address):
        payload = (
            f"factory-dashboard.deploy.v1:"
            f"{self.deploy_factory_address.lower()}:"
            f"{strategy_address.lower()}:"
            f"{want_address.lower()}:"
            f"{self.deploy_governance_address.lower()}"
        )
        return Web3.keccak(text=payload).hex()

    def _build_create_auction_tx(
        self,
        *,
        want_address,
        receiver_address,
        governance_address,
        starting_price,
        salt,
    ):
        try:
            factory = self.web3.eth.contract(
                address=self.web3.to_checksum_address(self.deploy_factory_address),
                abi=SINGLE_AUCTION_FACTORY_ABI,
            )
            create_fn = factory.functions.createNewAuction(
                self.web3.to_checksum_address(want_address),
                self.web3.to_checksum_address(receiver_address),
                self.web3.to_checksum_address(governance_address),
                int(starting_price),
                HexBytes(salt),
            )
            tx_data = create_fn._encode_transaction_data()
        except Exception as exc:
            raise FactoryDashboardError(f"Unable to build deploy transaction: {exc}", status_code=502) from exc

        return None, tx_data

    def _http_get_json(
        self,
        url,
        *,
        headers,
        timeout_seconds=None,
        error_context="Request",
        not_found_returns_none=False,
    ):
        request = Request(url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=timeout_seconds or self.deploy_price_timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404 and not_found_returns_none:
                return None
            payload = exc.read().decode("utf-8", errors="ignore")
            raise FactoryDashboardError(
                f"{error_context} failed with HTTP {exc.code}: {payload or exc.reason}",
                status_code=502,
            ) from exc
        except URLError as exc:
            raise FactoryDashboardError(f"{error_context} failed: {exc.reason}", status_code=502) from exc
        except json.JSONDecodeError as exc:
            raise FactoryDashboardError(f"{error_context} response was not valid JSON", status_code=502) from exc

    @staticmethod
    def _parse_decimal(value):
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _normalize_address(address):
        if not address or not Web3.is_address(address):
            raise FactoryDashboardError("Invalid address", status_code=400)
        return Web3.to_checksum_address(address)

    @staticmethod
    def _optional_normalize_address(address):
        if not address:
            return None
        return FactoryDashboardService._normalize_address(address)

    def _group_kicks(self, kick_rows):
        kicks_by_source = {}
        for row in kick_rows:
            if (row["operation_type"] or "kick") == "settle":
                continue
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
                    "sourceAddress": self._optional_normalize_address(detail_row["source_address"]),
                    "sourceName": detail_row["source_name"],
                    "contextType": detail_row["context_type"],
                    "contextAddress": self._optional_normalize_address(detail_row["context_address"]),
                    "contextName": detail_row["context_name"],
                    "contextSymbol": detail_row["context_symbol"],
                    "strategyAddress": self._optional_normalize_address(detail_row["strategy_address"]),
                    "strategyName": detail_row["strategy_name"],
                    "vaultAddress": self._optional_normalize_address(detail_row["vault_address"]),
                    "vaultName": detail_row["vault_name"],
                    "vaultSymbol": detail_row["vault_symbol"],
                    "auctionAddress": self._optional_normalize_address(detail_row["auction_address"]),
                    "auctionVersion": detail_row["auction_version"],
                    "wantAddress": self._optional_normalize_address(detail_row["want_address"]),
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
                    "tokenAddress": self._optional_normalize_address(detail_row["token_address"]),
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
                    "tokenAddress": self._normalize_address(token_address),
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
            "kick_txs.operation_type": self._has_column(conn, "kick_txs", "operation_type"),
            "kick_txs.source_type": self._has_column(conn, "kick_txs", "source_type"),
            "kick_txs.source_address": self._has_column(conn, "kick_txs", "source_address"),
            "kick_txs.token_symbol": self._has_column(conn, "kick_txs", "token_symbol"),
            "kick_txs.want_symbol": self._has_column(conn, "kick_txs", "want_symbol"),
            "kick_txs.auctionscan_round_id": self._has_column(conn, "kick_txs", "auctionscan_round_id"),
            "kick_txs.auctionscan_last_checked_at": self._has_column(conn, "kick_txs", "auctionscan_last_checked_at"),
            "kick_txs.auctionscan_matched_at": self._has_column(conn, "kick_txs", "auctionscan_matched_at"),
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
        if schema_features["kick_txs.operation_type"]:
            operation_type_expr = "COALESCE(k.operation_type, 'kick')"
        else:
            operation_type_expr = "'kick'"

        if schema_features["kick_txs.source_type"]:
            source_type_expr = "COALESCE(k.source_type, CASE WHEN k.strategy_address IS NOT NULL THEN 'strategy' END)"
        else:
            source_type_expr = "'strategy'"

        if schema_features["kick_txs.source_address"]:
            source_address_expr = "COALESCE(k.source_address, k.strategy_address)"
        else:
            source_address_expr = "k.strategy_address"

        return operation_type_expr, source_type_expr, source_address_expr

    def _build_kicks_sql(self, schema_features):
        operation_type_expr, source_type_expr, source_address_expr = self._build_kick_source_expressions(schema_features)
        kick_token_symbol_column = "COALESCE(k.token_symbol, t.symbol)" if schema_features["kick_txs.token_symbol"] else "t.symbol"
        return KICKS_SQL_TEMPLATE.format(
            operation_type_expr=operation_type_expr,
            source_type_expr=source_type_expr,
            source_address_expr=source_address_expr,
            kick_token_symbol_column=kick_token_symbol_column,
        )

    def _build_kicks_detail_sql(self, schema_features, include_status_filter=False):
        operation_type_expr, source_type_expr, source_address_expr = self._build_kick_source_expressions(schema_features)
        kick_token_symbol_column = "COALESCE(k.token_symbol, t.symbol)" if schema_features["kick_txs.token_symbol"] else "t.symbol"
        kick_auctionscan_round_id_column = (
            "k.auctionscan_round_id" if schema_features["kick_txs.auctionscan_round_id"] else "NULL"
        )
        kick_auctionscan_last_checked_at_column = (
            "k.auctionscan_last_checked_at" if schema_features["kick_txs.auctionscan_last_checked_at"] else "NULL"
        )
        kick_auctionscan_matched_at_column = (
            "k.auctionscan_matched_at" if schema_features["kick_txs.auctionscan_matched_at"] else "NULL"
        )
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
            operation_type_expr=operation_type_expr,
            source_type_expr=source_type_expr,
            source_address_expr=source_address_expr,
            source_name_column=source_name_column,
            fee_burner_join=fee_burner_join,
            kick_token_symbol_column=kick_token_symbol_column,
            kick_want_symbol_column=kick_want_symbol_column,
            kick_auctionscan_round_id_column=kick_auctionscan_round_id_column,
            kick_auctionscan_last_checked_at_column=kick_auctionscan_last_checked_at_column,
            kick_auctionscan_matched_at_column=kick_auctionscan_matched_at_column,
            want_token_join=want_token_join,
            status_clause=status_clause,
        )

    def _persist_kick_auctionscan_match(self, kick_id, *, round_id, checked_at, matched_at):
        with self._connect_rw() as conn:
            conn.execute(
                """
                UPDATE kick_txs
                SET auctionscan_round_id = ?, auctionscan_last_checked_at = ?, auctionscan_matched_at = ?
                WHERE id = ?
                """,
                (int(round_id), checked_at, matched_at, kick_id),
            )

    def _persist_kick_auctionscan_check(self, kick_id, *, checked_at):
        with self._connect_rw() as conn:
            conn.execute(
                """
                UPDATE kick_txs
                SET auctionscan_last_checked_at = ?
                WHERE id = ?
                """,
                (checked_at, kick_id),
            )

    def _build_auctionscan_auction_url(self, auction_address):
        if not auction_address:
            return None
        return f"{self.auctionscan_base_url}/auction/{self.deploy_chain_id}/{self._normalize_address(auction_address)}"

    def _build_auctionscan_round_url(self, auction_address, round_id):
        if not auction_address or round_id is None:
            return None
        return (
            f"{self.auctionscan_base_url}/round/{self.deploy_chain_id}/"
            f"{self._normalize_address(auction_address)}/{int(round_id)}"
        )

    def _build_kick_auctionscan_response(self, kick, *, resolved, cached):
        return {
            "kickId": kick["id"],
            "chainId": self.deploy_chain_id,
            "eligible": bool(kick["eligible"]),
            "resolved": bool(resolved),
            "cached": bool(cached),
            "auctionAddress": kick["auction_address"],
            "roundId": kick["auctionscan_round_id"],
            "auctionUrl": self._build_auctionscan_auction_url(kick["auction_address"]),
            "roundUrl": self._build_auctionscan_round_url(kick["auction_address"], kick["auctionscan_round_id"]),
            "lastCheckedAt": kick["auctionscan_last_checked_at"],
            "matchedAt": kick["auctionscan_matched_at"],
        }

    def _should_skip_auctionscan_recheck(self, last_checked_at):
        if not last_checked_at or self.auctionscan_recheck_seconds <= 0:
            return False
        checked_at = self._parse_timestamp(last_checked_at)
        if checked_at is None:
            return False
        delta = datetime.now(timezone.utc) - checked_at
        return delta.total_seconds() < self.auctionscan_recheck_seconds

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

    @staticmethod
    def _parse_timestamp(value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

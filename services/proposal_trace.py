"""Independent, read-only adapter for Proposal Trace contract version 1."""

import base64
import binascii
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
from urllib.parse import quote


CONTRACT_VERSION = 1
DEFAULT_LIMIT = 25
MAX_LIMIT = 100
MAX_CURSOR_LENGTH = 1024
MAX_UINT256 = 2**256 - 1

_ADDRESS = re.compile(r"0x[0-9a-f]{40}")
_HASH = re.compile(r"0x[0-9a-f]{64}")
_HEX = re.compile(r"0x(?:[0-9a-fA-F]{2})*")
_GIST = re.compile(r"https://gist\.wavey\.info/[A-Za-z0-9]{16,64}")
_REQUIRED_COLUMNS = {
    "source_id": "TEXT",
    "external_id": "TEXT",
    "created_at": "INTEGER",
    "item_json": "TEXT",
    "analysis_json": "TEXT",
    "report_markdown": "TEXT",
    "gist_url": "TEXT",
}
_ITEM_FIELDS = {
    "source_id",
    "external_id",
    "kind",
    "title",
    "description",
    "created_at",
    "observation_block",
    "source_record",
    "facts",
    "links",
    "actions",
    "unknowns",
}
_ANALYSIS_FIELDS = {
    "disposition",
    "severity",
    "summary",
    "findings",
    "unknowns",
    "checks",
    "decoded_actions",
}


class InvalidRequest(ValueError):
    pass


class NotFound(LookupError):
    pass


class DataUnavailable(RuntimeError):
    pass


def parse_source_path(source_path):
    parts = source_path.split("/")
    if parts in (
        ["resupply", "governance"],
        ["curve", "ownership"],
        ["curve", "parameter"],
    ):
        return ":".join(parts)
    if (
        len(parts) == 3
        and parts[0] == "safe"
        and parts[1] == "1"
        and _ADDRESS.fullmatch(parts[2])
    ):
        return ":".join(parts)
    raise NotFound("source not found")


def source_path(source_id):
    return source_id.replace(":", "/")


def canonical_external_id(source_id, external_id):
    if source_id.startswith("safe:"):
        if _HASH.fullmatch(external_id) is None:
            raise InvalidRequest("Safe transaction hash is not canonical")
        return external_id
    if not external_id.isdecimal():
        raise InvalidRequest("audit ID is not canonical")
    canonical = str(int(external_id))
    if canonical != external_id:
        raise InvalidRequest("audit ID is not canonical")
    return canonical


def canonical_nonce(value):
    if not value.isdecimal() or str(int(value)) != value:
        raise InvalidRequest("nonce is not canonical")
    parsed = int(value)
    if parsed > MAX_UINT256:
        raise InvalidRequest("nonce is outside uint256")
    return parsed


def parse_limit(value):
    if value is None:
        return DEFAULT_LIMIT
    if not value.isdecimal() or str(int(value)) != value:
        raise InvalidRequest("limit is invalid")
    parsed = int(value)
    if not 1 <= parsed <= MAX_LIMIT:
        raise InvalidRequest("limit is invalid")
    return parsed


def _encode_cursor(created_at, external_id):
    raw = json.dumps(
        [CONTRACT_VERSION, created_at, external_id],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(value):
    if value is None:
        return None
    if (
        not value
        or len(value) > MAX_CURSOR_LENGTH
        or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None
    ):
        raise InvalidRequest("cursor is invalid")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
        parsed = json.loads(decoded.decode("ascii"))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidRequest("cursor is invalid") from exc
    if (
        not isinstance(parsed, list)
        or len(parsed) != 3
        or parsed[0] != CONTRACT_VERSION
        or type(parsed[1]) is not int
        or parsed[1] < 0
        or not isinstance(parsed[2], str)
        or not parsed[2]
        or _encode_cursor(parsed[1], parsed[2]) != value
    ):
        raise InvalidRequest("cursor is invalid")
    return parsed[1], parsed[2]


def _database_path(value):
    if not isinstance(value, str) or not value.strip():
        raise DataUnavailable("Proposal Trace database is not configured")
    path = Path(value)
    try:
        valid = (
            path.is_absolute()
            and not path.is_symlink()
            and path.is_file()
            and path.resolve(strict=True) == path
        )
    except OSError as exc:
        raise DataUnavailable("Proposal Trace database is unavailable") from exc
    if not valid:
        raise DataUnavailable("Proposal Trace database path is invalid")
    return path


def _validate_schema(connection):
    query_only = connection.execute("PRAGMA query_only").fetchone()[0]
    columns = {
        str(row[1]): str(row[2]).upper()
        for row in connection.execute("PRAGMA table_info(audit_items)").fetchall()
    }
    if query_only != 1 or any(
        columns.get(name) != kind for name, kind in _REQUIRED_COLUMNS.items()
    ):
        raise DataUnavailable("Proposal Trace contract version is incompatible")


@contextmanager
def _connection(db_path, busy_timeout_ms):
    path = _database_path(db_path)
    if type(busy_timeout_ms) is not int or not 0 <= busy_timeout_ms <= 60_000:
        raise DataUnavailable("Proposal Trace busy timeout is invalid")
    uri = "file:{}?mode=ro".format(quote(str(path), safe="/"))
    connection = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=busy_timeout_ms / 1000)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout={}".format(busy_timeout_ms))
        _validate_schema(connection)
        yield connection
    except DataUnavailable:
        raise
    except (OSError, sqlite3.Error) as exc:
        raise DataUnavailable("Proposal Trace database read failed") from exc
    finally:
        if connection is not None:
            connection.close()


def _json_object(raw, label):
    if not isinstance(raw, str):
        raise DataUnavailable("Proposal Trace {} is invalid".format(label))
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DataUnavailable("Proposal Trace {} is invalid".format(label)) from exc
    if not isinstance(value, dict):
        raise DataUnavailable("Proposal Trace {} is invalid".format(label))
    return value


def _strings(value):
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _validate_fact(value):
    return (
        isinstance(value, dict)
        and set(value) == {"value", "url"}
        and isinstance(value["value"], str)
        and (value["url"] is None or isinstance(value["url"], str))
    )


def _validate_action(value):
    return (
        isinstance(value, dict)
        and set(value)
        == {
            "index",
            "executor",
            "target",
            "value_wei",
            "calldata",
            "operation",
            "raw",
            "unresolved",
        }
        and type(value["index"]) is int
        and value["index"] >= 0
        and isinstance(value["executor"], str)
        and isinstance(value["target"], str)
        and type(value["value_wei"]) is int
        and 0 <= value["value_wei"] <= MAX_UINT256
        and isinstance(value["calldata"], str)
        and _HEX.fullmatch(value["calldata"]) is not None
        and value["operation"] in {"CALL", "DELEGATECALL", "UNKNOWN"}
        and (
            value["raw"] is None
            or (isinstance(value["raw"], str) and _HEX.fullmatch(value["raw"]))
        )
        and (value["unresolved"] is None or isinstance(value["unresolved"], str))
    )


def _validate_check(value):
    valid = (
        isinstance(value, dict)
        and set(value) == {"id", "action_index", "status", "summary", "evidence"}
        and isinstance(value["id"], str)
        and type(value["action_index"]) is int
        and value["action_index"] >= 0
        and value["status"] in {"PASS", "FAIL", "UNKNOWN"}
        and isinstance(value["summary"], str)
        and isinstance(value["evidence"], list)
    )
    if not valid:
        return False
    return all(
        isinstance(evidence, dict)
        and set(evidence) == {"chain_id", "block", "target", "request", "raw_result"}
        and type(evidence["chain_id"]) is int
        and evidence["chain_id"] > 0
        and type(evidence["block"]) is int
        and evidence["block"] >= 0
        and all(
            isinstance(evidence[field], str)
            for field in ("target", "request", "raw_result")
        )
        for evidence in value["evidence"]
    )


def _validate_decoded_action(value):
    if (
        not isinstance(value, dict)
        or set(value) != {"action_index", "function", "inputs"}
        or type(value["action_index"]) is not int
        or value["action_index"] < 0
        or not isinstance(value["function"], str)
        or not isinstance(value["inputs"], list)
    ):
        return False
    return all(
        isinstance(item, dict)
        and set(item) == {"name", "type", "value"}
        and all(isinstance(item[field], str) for field in ("name", "type", "value"))
        for item in value["inputs"]
    )


def _validated_row(row):
    item = _json_object(row["item_json"], "item")
    analysis = _json_object(row["analysis_json"], "analysis")
    facts = item.get("facts")
    links = item.get("links")
    actions = item.get("actions")
    decoded = analysis.get("decoded_actions")
    checks = analysis.get("checks")
    if (
        set(item) != _ITEM_FIELDS
        or item.get("source_id") != row["source_id"]
        or item.get("external_id") != row["external_id"]
        or not isinstance(item.get("kind"), str)
        or not isinstance(item.get("title"), str)
        or not isinstance(item.get("description"), str)
        or type(item.get("created_at")) is not int
        or item["created_at"] != row["created_at"]
        or item["created_at"] < 0
        or type(item.get("observation_block")) is not int
        or item["observation_block"] < 0
        or not isinstance(item.get("source_record"), dict)
        or not isinstance(facts, dict)
        or not all(
            isinstance(key, str) and _validate_fact(value)
            for key, value in facts.items()
        )
        or not isinstance(links, dict)
        or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in links.items()
        )
        or not isinstance(actions, list)
        or not all(_validate_action(action) for action in actions)
        or [action["index"] for action in actions] != list(range(len(actions)))
        or not _strings(item.get("unknowns"))
        or set(analysis) != _ANALYSIS_FIELDS
        or analysis.get("disposition") not in {"completed", "fallback"}
        or analysis.get("severity") not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        or not isinstance(analysis.get("summary"), str)
        or not analysis["summary"]
        or not _strings(analysis.get("findings"))
        or not _strings(analysis.get("unknowns"))
        or not isinstance(checks, list)
        or not all(_validate_check(check) for check in checks)
        or not isinstance(decoded, list)
        or not all(_validate_decoded_action(action) for action in decoded)
        or len({action["action_index"] for action in decoded}) != len(decoded)
    ):
        raise DataUnavailable("Proposal Trace row is incompatible")
    report = row["report_markdown"]
    gist_url = row["gist_url"]
    if report is not None and (
        not isinstance(report, str) or not report.startswith("# ") or not report.strip()
    ):
        raise DataUnavailable("Proposal Trace report is invalid")
    if gist_url is not None and (
        not isinstance(gist_url, str) or _GIST.fullmatch(gist_url) is None
    ):
        raise DataUnavailable("Proposal Trace Gist URL is invalid")
    return item, analysis


def _timestamp(value):
    try:
        return (
            datetime.fromtimestamp(value, timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise DataUnavailable("Proposal Trace timestamp is invalid") from exc


def _links(source_id, external_id, gist_url):
    result = {
        "self": "/api/proposal-trace/{}/audits/{}".format(
            source_path(source_id), external_id
        )
    }
    if gist_url is not None:
        result["gist"] = gist_url
    return result


def _safe_projection(item):
    if item["kind"] != "safe":
        return None
    transaction = item["source_record"].get("transaction")
    parts = item["source_id"].split(":")
    if not isinstance(transaction, dict) or len(parts) != 3:
        raise DataUnavailable("Proposal Trace Safe item is incompatible")
    nonce = transaction.get("nonce")
    execution_hash = transaction.get("transactionHash")
    if (
        type(nonce) is not int
        or not 0 <= nonce <= MAX_UINT256
        or transaction.get("safeTxHash") != item["external_id"]
        or type(transaction.get("isExecuted")) is not bool
        or (
            execution_hash is not None
            and (
                not isinstance(execution_hash, str)
                or _HASH.fullmatch(execution_hash) is None
            )
        )
    ):
        raise DataUnavailable("Proposal Trace Safe item is incompatible")
    return {
        "chain_id": int(parts[1]),
        "safe_address": parts[2],
        "safe_tx_hash": item["external_id"],
        "nonce": str(nonce),
        "is_executed": transaction["isExecuted"],
        "execution_tx_hash": execution_hash,
    }


def _compact(row, item, analysis):
    result = {
        "source_id": row["source_id"],
        "external_id": row["external_id"],
        "created_at": _timestamp(row["created_at"]),
        "severity": analysis["severity"],
        "summary": analysis["summary"],
        "links": _links(row["source_id"], row["external_id"], row["gist_url"]),
    }
    safe = _safe_projection(item)
    if safe is not None:
        result["safe"] = {"nonce": safe["nonce"]}
    return result


def _detail(row, item, analysis):
    decoded = {action["action_index"]: action for action in analysis["decoded_actions"]}
    actions = []
    for action in item["actions"]:
        projected = {
            "index": action["index"],
            "operation": action["operation"],
            "executor": action["executor"],
            "target": action["target"],
            "value_wei": str(action["value_wei"]),
            "calldata": action["calldata"],
            "raw": action["raw"],
            "unresolved": action["unresolved"],
            "decoded": decoded.get(action["index"]),
        }
        actions.append(projected)
    result = {
        "contract_version": CONTRACT_VERSION,
        "source_id": row["source_id"],
        "external_id": row["external_id"],
        "kind": item["kind"],
        "title": item["title"],
        "description": item["description"],
        "created_at": _timestamp(row["created_at"]),
        "observation_block": item["observation_block"],
        "facts": item["facts"],
        "analysis": {
            "disposition": analysis["disposition"],
            "severity": analysis["severity"],
            "summary": analysis["summary"],
            "findings": analysis["findings"],
            "unknowns": analysis["unknowns"],
            "checks": [
                {
                    "id": check["id"],
                    "action_index": check["action_index"],
                    "status": check["status"],
                    "summary": check["summary"],
                }
                for check in analysis["checks"]
            ],
        },
        "actions": actions,
        "report_markdown": row["report_markdown"],
        "links": _links(row["source_id"], row["external_id"], row["gist_url"]),
    }
    safe = _safe_projection(item)
    if safe is not None:
        result["safe"] = safe
    return result


class ProposalTraceService:
    def __init__(self, db_path, busy_timeout_ms=5000):
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms

    def list_audits(self, source_id, limit_value=None, cursor_value=None):
        limit = parse_limit(limit_value)
        cursor = _decode_cursor(cursor_value)
        parameters = [source_id]
        predicate = "source_id=? AND analysis_json IS NOT NULL"
        if cursor is not None:
            predicate += " AND (created_at < ? OR (created_at = ? AND external_id > ?))"
            parameters.extend([cursor[0], cursor[0], cursor[1]])
        parameters.append(limit + 1)
        query = (
            "SELECT source_id, external_id, created_at, item_json, analysis_json, "
            "report_markdown, gist_url FROM audit_items WHERE {} "
            "ORDER BY created_at DESC, external_id ASC LIMIT ?".format(predicate)
        )
        with _connection(self.db_path, self.busy_timeout_ms) as connection:
            rows = connection.execute(query, parameters).fetchall()
        projected = []
        for row in rows[:limit]:
            item, analysis = _validated_row(row)
            projected.append(_compact(row, item, analysis))
        next_cursor = None
        if len(rows) > limit:
            last = rows[limit - 1]
            next_cursor = _encode_cursor(last["created_at"], last["external_id"])
        return {
            "contract_version": CONTRACT_VERSION,
            "source_id": source_id,
            "audits": projected,
            "next_cursor": next_cursor,
        }

    def audit(self, source_id, external_id):
        external_id = canonical_external_id(source_id, external_id)
        query = (
            "SELECT source_id, external_id, created_at, item_json, analysis_json, "
            "report_markdown, gist_url FROM audit_items "
            "WHERE source_id=? AND external_id=? AND analysis_json IS NOT NULL"
        )
        with _connection(self.db_path, self.busy_timeout_ms) as connection:
            row = connection.execute(query, (source_id, external_id)).fetchone()
        if row is None:
            raise NotFound("audit not found")
        item, analysis = _validated_row(row)
        return _detail(row, item, analysis)

    def safe_audit_by_nonce(self, chain_id, safe_address, nonce_value):
        source_id = parse_source_path("safe/{}/{}".format(chain_id, safe_address))
        nonce = canonical_nonce(nonce_value)
        query = (
            "SELECT source_id, external_id, created_at, item_json, analysis_json, "
            "report_markdown, gist_url FROM audit_items "
            "WHERE source_id=? AND analysis_json IS NOT NULL "
            "AND CAST(json_extract(item_json, '$.source_record.transaction.nonce') AS TEXT)=? "
            "ORDER BY created_at DESC, external_id ASC LIMIT 1"
        )
        with _connection(self.db_path, self.busy_timeout_ms) as connection:
            row = connection.execute(query, (source_id, str(nonce))).fetchone()
        if row is None:
            raise NotFound("audit not found")
        item, analysis = _validated_row(row)
        return source_id, _detail(row, item, analysis)

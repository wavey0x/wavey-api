import json
from pathlib import Path
import sqlite3

from flask import Flask
import pytest

from proposal_trace_routes import proposal_trace_api
from services.proposal_trace import (
    DataUnavailable,
    InvalidRequest,
    ProposalTraceService,
    canonical_nonce,
)
from services.proposal_trace_policy import (
    DISABLED,
    PUBLIC,
    SOURCE_POLICIES,
    TRACKED_SOURCE_IDS,
    exposure_policy,
)


GIST_URL = "https://gist.wavey.info/AbCdEfGhIjKlMnOp"
SAFE_ADDRESS = "0xc420c9d507d0e038bd76383aaadcad576ed0073c"
SAFE_SOURCE = "safe:1:" + SAFE_ADDRESS
HASH_A = "0x" + "11" * 32
HASH_B = "0x" + "22" * 32


def _analysis(summary="Reviewed"):
    return {
        "disposition": "completed",
        "severity": "MEDIUM",
        "summary": summary,
        "findings": ["One finding"],
        "unknowns": ["One unknown"],
        "checks": [
            {
                "id": "authority",
                "action_index": 0,
                "status": "PASS",
                "summary": "Authority matched",
                "evidence": [
                    {
                        "chain_id": 1,
                        "block": 123,
                        "target": "0x" + "33" * 20,
                        "request": "0x1234",
                        "raw_result": "0xabcd",
                    }
                ],
            }
        ],
        "decoded_actions": [
            {
                "action_index": 0,
                "function": "transfer(address,uint256)",
                "inputs": [{"name": "amount", "type": "uint256", "value": "7"}],
            }
        ],
    }


def _item(source_id, external_id, created_at, nonce=None):
    is_safe = source_id.startswith("safe:")
    source_record = {}
    facts = {"proposer": {"value": "0x1234", "url": None}}
    kind = "curve"
    if is_safe:
        source_record = {
            "transaction": {
                "safeTxHash": external_id,
                "nonce": nonce,
                "isExecuted": False,
                "transactionHash": None,
                "signatures": ["must-not-leak"],
            },
            "safe_state": {"owners": ["must-not-leak"]},
        }
        facts = {
            "safe": {"value": SAFE_ADDRESS, "url": None},
            "nonce": {"value": str(nonce), "url": None},
        }
        kind = "safe"
    else:
        source_record = {"proposal_id": int(external_id)}
    return {
        "source_id": source_id,
        "external_id": external_id,
        "kind": kind,
        "title": "Audit " + external_id,
        "description": "Description",
        "created_at": created_at,
        "observation_block": 123,
        "source_record": source_record,
        "facts": facts,
        "links": {"source": "https://example.invalid/not-projected"},
        "actions": [
            {
                "index": 0,
                "executor": "0x" + "44" * 20,
                "target": "0x" + "55" * 20,
                "value_wei": 7,
                "calldata": "0x1234",
                "operation": "CALL",
                "raw": "0x1234",
                "unresolved": None,
            }
        ],
        "unknowns": [],
    }


def _database(tmp_path):
    path = tmp_path / "state.db"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE audit_items ("
        "source_id TEXT NOT NULL, external_id TEXT NOT NULL, status TEXT NOT NULL, "
        "item_json TEXT NOT NULL, analysis_json TEXT, "
        "gist_url TEXT, telegram_message_id INTEGER, session_workspace TEXT, "
        "review_reason TEXT, report_markdown TEXT, "
        "PRIMARY KEY (source_id, external_id))"
    )
    connection.commit()
    connection.close()
    return path


def _insert(path, source_id, external_id, created_at, nonce=None, report="# Stored\n"):
    item = _item(source_id, external_id, created_at, nonce)
    connection = sqlite3.connect(path)
    connection.execute(
        "INSERT INTO audit_items "
        "(source_id, external_id, status, item_json, analysis_json, gist_url, "
        "report_markdown) VALUES (?, ?, 'sent', ?, ?, ?, ?)",
        (
            source_id,
            external_id,
            json.dumps(item, sort_keys=True, separators=(",", ":")),
            json.dumps(_analysis(), sort_keys=True, separators=(",", ":")),
            GIST_URL,
            report,
        ),
    )
    connection.commit()
    connection.close()


def _client(path):
    app = Flask(__name__)
    app.config.update(
        TESTING=True,
        PROPOSAL_TRACE_DB_PATH=str(path),
        PROPOSAL_TRACE_BUSY_TIMEOUT_MS=100,
    )
    app.register_blueprint(proposal_trace_api, url_prefix="/api")
    return app.test_client()


def test_source_policy_is_complete_and_unknown_sources_fail_closed():
    assert set(SOURCE_POLICIES) == set(TRACKED_SOURCE_IDS)
    assert all(exposure_policy(source_id) == PUBLIC for source_id in TRACKED_SOURCE_IDS)
    assert exposure_policy("safe:1:0x" + "00" * 20) == DISABLED


def test_list_is_stable_cursor_paginated_and_compact(tmp_path):
    path = _database(tmp_path)
    _insert(path, "curve:ownership", "2", 200)
    _insert(path, "curve:ownership", "1", 100)
    _insert(path, "curve:parameter", "9", 300)

    service = ProposalTraceService(str(path), 100)
    first = service.list_audits("curve:ownership", "1")
    assert [item["external_id"] for item in first["audits"]] == ["2"]
    assert first["next_cursor"]
    assert "report_markdown" not in first["audits"][0]
    assert first["audits"][0]["links"]["gist"] == GIST_URL

    second = service.list_audits("curve:ownership", "1", first["next_cursor"])
    assert [item["external_id"] for item in second["audits"]] == ["1"]
    assert second["next_cursor"] is None
    with pytest.raises(InvalidRequest):
        service.list_audits("curve:ownership", "01")
    with pytest.raises(InvalidRequest):
        service.list_audits("curve:ownership", "1", first["next_cursor"] + "=")


def test_detail_projects_complete_actions_without_internal_records(tmp_path):
    path = _database(tmp_path)
    _insert(path, "curve:ownership", "7", 200, report=None)

    detail = ProposalTraceService(str(path), 100).audit("curve:ownership", "7")
    assert detail["contract_version"] == 1
    assert detail["report_markdown"] is None
    assert detail["actions"][0]["value_wei"] == "7"
    assert detail["actions"][0]["calldata"] == "0x1234"
    assert detail["actions"][0]["decoded"]["function"] == "transfer(address,uint256)"
    assert "evidence" not in detail["analysis"]["checks"][0]
    encoded = json.dumps(detail)
    assert "source_record" not in encoded
    assert "signatures" not in encoded
    assert "not-projected" not in encoded


def test_safe_nonce_route_uses_newest_then_full_hash_ascending(tmp_path):
    path = _database(tmp_path)
    _insert(path, SAFE_SOURCE, HASH_B, 300, nonce=8)
    _insert(path, SAFE_SOURCE, HASH_A, 300, nonce=8)

    client = _client(path)
    response = client.get(
        "/api/proposal-trace/safe/1/{}/audits/by-nonce/8".format(SAFE_ADDRESS)
    )
    assert response.status_code == 200
    assert response.get_json()["external_id"] == HASH_A
    assert response.get_json()["safe"] == {
        "chain_id": 1,
        "safe_address": SAFE_ADDRESS,
        "safe_tx_hash": HASH_A,
        "nonce": "8",
        "is_executed": False,
        "execution_tx_hash": None,
    }
    assert response.headers["Cache-Control"] == "public, max-age=30"


def test_routes_reject_noncanonical_input_and_missing_rows(tmp_path):
    path = _database(tmp_path)
    client = _client(path)

    assert (
        client.get("/api/proposal-trace/curve/ownership/audits/01").status_code == 400
    )
    assert client.get("/api/proposal-trace/curve/ownership/audits/1").status_code == 404
    assert (
        client.get(
            "/api/proposal-trace/safe/1/{}/audits/by-nonce/01".format(SAFE_ADDRESS)
        ).status_code
        == 400
    )
    assert client.get("/api/proposal-trace/curve/unknown/audits").status_code == 404
    assert (
        client.get("/api/proposal-trace/curve/ownership/audits?extra=1").status_code
        == 400
    )


def test_incompatible_or_noncanonical_database_fails_closed(tmp_path):
    path = _database(tmp_path)
    _insert(path, "curve:ownership", "1", 100)
    connection = sqlite3.connect(path)
    connection.execute("UPDATE audit_items SET analysis_json='{}'")
    connection.commit()
    connection.close()
    with pytest.raises(DataUnavailable):
        ProposalTraceService(str(path), 100).audit("curve:ownership", "1")

    symlink = tmp_path / "linked.db"
    symlink.symlink_to(path)
    with pytest.raises(DataUnavailable):
        ProposalTraceService(str(symlink), 100).list_audits("curve:ownership")


def test_nonce_accepts_full_uint256_range():
    assert canonical_nonce(str(2**256 - 1)) == 2**256 - 1
    with pytest.raises(InvalidRequest):
        canonical_nonce(str(2**256))


def test_minimal_wsgi_app_registers_only_proposal_trace_routes(tmp_path, monkeypatch):
    path = _database(tmp_path)
    _insert(path, "curve:ownership", "1", 100)
    monkeypatch.setenv("PROPOSAL_TRACE_DB_PATH", str(path))

    from proposal_trace_app import create_app

    app = create_app()
    rules = {
        rule.rule for rule in app.url_map.iter_rules() if rule.endpoint != "static"
    }
    assert rules == {
        "/api/proposal-trace/<path:source_path>/audits",
        "/api/proposal-trace/<path:source_path>/audits/<external_id>",
        "/api/proposal-trace/safe/<chain_id>/<safe_address>/audits/by-nonce/<nonce>",
    }
    assert (
        app.test_client().get("/api/proposal-trace/curve/ownership/audits").status_code
        == 200
    )


def test_claw_unit_is_loopback_only_and_cannot_read_private_proposal_trace_state():
    unit = Path("deploy/systemd/proposal-trace-api.service").read_text()
    remote = Path("ops/deploy-proposal-trace-api-remote.sh").read_text()

    assert "--bind 127.0.0.1:3101" in unit
    assert "SupplementaryGroups=proposal-trace-reader" in unit
    assert "IPAddressDeny=any" in unit
    assert "IPAddressAllow=localhost" in unit
    assert "/etc/proposal-trace" in unit
    assert "/var/lib/proposal-trace/.codex" in unit
    assert "/var/lib/proposal-trace-workspaces" in unit
    assert 'chmod 0710 "$STATE_ROOT"' in remote
    assert 'chmod 0640 "$DATABASE"' in remote
    assert "grep -Fx proposal-trace" in remote

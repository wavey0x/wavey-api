import json

import pytest

from services.crvlol_snapshot import get_snapshot_path, load_snapshot


def test_snapshot_path_is_required(monkeypatch):
    monkeypatch.delenv("CRVLOL_SNAPSHOT_PATH", raising=False)

    with pytest.raises(RuntimeError, match="CRVLOL_SNAPSHOT_PATH must be set"):
        get_snapshot_path()


def test_load_snapshot_from_configured_path(monkeypatch, tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps({"ll_data": {"cvxCRV": {}}}))
    monkeypatch.setenv("CRVLOL_SNAPSHOT_PATH", str(snapshot_path))

    assert load_snapshot() == {"ll_data": {"cvxCRV": {}}}


def test_load_snapshot_rejects_non_object(monkeypatch, tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text("[]")
    monkeypatch.setenv("CRVLOL_SNAPSHOT_PATH", str(snapshot_path))

    with pytest.raises(ValueError, match="must be a JSON object"):
        load_snapshot()

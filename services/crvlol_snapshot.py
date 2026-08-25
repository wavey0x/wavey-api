import json
import os
from pathlib import Path


def get_snapshot_path() -> Path:
    value = os.getenv("CRVLOL_SNAPSHOT_PATH")
    if not value or not value.strip():
        raise RuntimeError("CRVLOL_SNAPSHOT_PATH must be set")
    return Path(value).expanduser()


def load_snapshot():
    with get_snapshot_path().open() as snapshot_file:
        snapshot = json.load(snapshot_file)
    if not isinstance(snapshot, dict):
        raise ValueError("CRV snapshot must be a JSON object")
    return snapshot

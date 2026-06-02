import argparse
import json
import os

from .auth import create_api_key, list_api_keys, revoke_api_key, rotate_api_key
from .db import gist_connection, init_gist_database


class _AppConfig:
    config = {
        "SQLITE_DB_PATH": os.getenv("SQLITE_DB_PATH"),
        "SQLITE_BUSY_TIMEOUT_MS": int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000")),
    }


def _app():
    if not _AppConfig.config["SQLITE_DB_PATH"]:
        raise RuntimeError("SQLITE_DB_PATH must be set")
    return _AppConfig


def _scopes(value):
    return [scope.strip() for scope in value.split(",") if scope.strip()]


def main(argv=None):
    parser = argparse.ArgumentParser(prog="admin")
    subparsers = parser.add_subparsers(dest="resource", required=True)
    keys = subparsers.add_parser("keys")
    key_commands = keys.add_subparsers(dest="command", required=True)

    create = key_commands.add_parser("create")
    create.add_argument("--domain", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--scopes", required=True, type=_scopes)

    list_cmd = key_commands.add_parser("list")
    list_cmd.add_argument("--domain")

    revoke = key_commands.add_parser("revoke")
    revoke.add_argument("key_prefix_or_id")

    rotate = key_commands.add_parser("rotate")
    rotate.add_argument("key_prefix_or_id")
    rotate.add_argument("--name")

    args = parser.parse_args(argv)
    app = _app()
    init_gist_database(app)

    with gist_connection(app) as conn:
        if args.command == "create":
            result = create_api_key(conn, args.domain, args.name, args.scopes)
            print(json.dumps(result, indent=2))
            print("Save this key now. It cannot be recovered.")
        elif args.command == "list":
            print(json.dumps(list_api_keys(conn, args.domain), indent=2))
        elif args.command == "revoke":
            revoke_api_key(conn, args.key_prefix_or_id)
            print(json.dumps({"revoked": True}, indent=2))
        elif args.command == "rotate":
            result = rotate_api_key(conn, args.key_prefix_or_id, args.name)
            print(json.dumps(result, indent=2))
            print("Save this key now. It cannot be recovered.")


if __name__ == "__main__":
    main()

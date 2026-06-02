import argparse
import json
import os

from .auth import create_api_key, list_api_keys, revoke_api_key, rotate_api_key
from .db import gist_connection, init_gist_database
from .service import rerender_gists


GIST_ROLE_SCOPES = {
    "user": ["gist:read", "gist:write"],
    "admin": ["gist:read", "gist:write", "gist:delete"],
}


class _AppConfig:
    config = {}


def _app():
    _AppConfig.config = {
        "SQLITE_DB_PATH": os.getenv("SQLITE_DB_PATH"),
        "SQLITE_BUSY_TIMEOUT_MS": int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000")),
    }
    if not _AppConfig.config["SQLITE_DB_PATH"]:
        raise RuntimeError("SQLITE_DB_PATH must be set")
    return _AppConfig


def _scopes(value):
    return [scope.strip() for scope in value.split(",") if scope.strip()]


def _resolve_create_args(args):
    if args.scopes:
        return args.domain or "gist", args.scopes
    if args.domain and args.domain != "gist":
        raise ValueError("--scopes is required when --domain is not gist")
    return "gist", GIST_ROLE_SCOPES[args.role or "user"]


def main(argv=None):
    parser = argparse.ArgumentParser(prog="admin")
    subparsers = parser.add_subparsers(dest="resource", required=True)
    keys = subparsers.add_parser("keys")
    key_commands = keys.add_subparsers(dest="command", required=True)

    create = key_commands.add_parser("create")
    create.add_argument("--domain")
    create.add_argument("--name", required=True)
    create.add_argument("--role", choices=sorted(GIST_ROLE_SCOPES), default="user")
    create.add_argument("--scopes", type=_scopes)

    list_cmd = key_commands.add_parser("list")
    list_cmd.add_argument("--domain")

    revoke = key_commands.add_parser("revoke")
    revoke.add_argument("key_prefix_or_id")

    rotate = key_commands.add_parser("rotate")
    rotate.add_argument("key_prefix_or_id")
    rotate.add_argument("--name")

    gists = subparsers.add_parser("gists")
    gist_commands = gists.add_subparsers(dest="command", required=True)

    rerender = gist_commands.add_parser("rerender")
    rerender_target = rerender.add_mutually_exclusive_group(required=True)
    rerender_target.add_argument("--id", dest="external_id")
    rerender_target.add_argument("--all", action="store_true")
    rerender.add_argument("--dry-run", action="store_true")

    args = parser.parse_args(argv)
    app = _app()
    init_gist_database(app)

    if args.resource == "keys":
        with gist_connection(app) as conn:
            if args.command == "create":
                try:
                    domain, scopes = _resolve_create_args(args)
                except ValueError as exc:
                    parser.error(str(exc))
                result = create_api_key(conn, domain, args.name, scopes)
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
    elif args.resource == "gists":
        if args.command == "rerender":
            result = rerender_gists(
                app,
                external_id=args.external_id,
                dry_run=args.dry_run,
            )
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

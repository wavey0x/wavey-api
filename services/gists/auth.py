import base64
import json
import re
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

from werkzeug.security import check_password_hash, generate_password_hash


KEY_RE = re.compile(
    r"^wapi_([a-z][a-z0-9-]{0,31})_([A-Za-z0-9_-]{8,})_([A-Za-z0-9_-]{43,})$"
)


@dataclass(frozen=True)
class AuthResult:
    key_id: int
    domain: str
    name: str
    key_prefix: str
    scopes: frozenset[str]


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _base64url_random(byte_count):
    return base64.urlsafe_b64encode(secrets.token_bytes(byte_count)).rstrip(b"=").decode(
        "ascii"
    )


def _parse_key(api_key):
    match = KEY_RE.match(api_key or "")
    if not match:
        return None
    domain, public_prefix, _secret = match.groups()
    return domain, f"wapi_{domain}_{public_prefix}"


def _normalize_key_input(domain, name, scopes):
    domain = (domain or "").strip()
    name = (name or "").strip()
    scopes = [scope.strip() for scope in scopes if scope and scope.strip()]

    if not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", domain):
        raise ValueError("domain must be a short lowercase slug")
    if not name:
        raise ValueError("name is required")
    if not scopes:
        raise ValueError("at least one scope is required")
    return domain, name, scopes


def _new_key_material(domain):
    public_prefix = _base64url_random(6)
    secret = _base64url_random(32)
    full_key = f"wapi_{domain}_{public_prefix}_{secret}"
    key_prefix = f"wapi_{domain}_{public_prefix}"
    key_hash = generate_password_hash(full_key)
    return full_key, key_prefix, key_hash


def create_api_key(conn, domain, name, scopes):
    domain, name, scopes = _normalize_key_input(domain, name, scopes)
    now = utc_now()
    for _ in range(8):
        full_key, key_prefix, key_hash = _new_key_material(domain)

        try:
            with conn:
                cursor = conn.execute(
                    """
                    insert into api_keys(domain, name, key_hash, key_prefix, scopes_json, created_at)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (domain, name, key_hash, key_prefix, json.dumps(scopes), now),
                )
            return {
                "id": cursor.lastrowid,
                "key": full_key,
                "key_prefix": key_prefix,
                "domain": domain,
                "name": name,
                "scopes": scopes,
                "created_at": now,
            }
        except sqlite3.IntegrityError:
            continue

    raise RuntimeError("could not allocate unique api key prefix")


def verify_api_key(conn, authorization_header, required_domain, required_scope):
    if not authorization_header or not authorization_header.startswith("Bearer "):
        return None, "unauthorized"

    api_key = authorization_header[len("Bearer ") :].strip()
    parsed = _parse_key(api_key)
    if not parsed:
        return None, "unauthorized"

    domain, key_prefix = parsed
    if domain != required_domain:
        return None, "unauthorized"

    row = conn.execute(
        """
        select id, domain, name, key_hash, key_prefix, scopes_json
        from api_keys
        where key_prefix = ? and revoked_at is null
        """,
        (key_prefix,),
    ).fetchone()
    if row is None or row["domain"] != required_domain:
        return None, "unauthorized"
    if not check_password_hash(row["key_hash"], api_key):
        return None, "unauthorized"

    scopes = frozenset(json.loads(row["scopes_json"]))
    if required_scope not in scopes:
        return None, "forbidden"

    with conn:
        conn.execute(
            "update api_keys set last_used_at = ? where id = ?",
            (utc_now(), row["id"]),
        )

    return (
        AuthResult(
            key_id=row["id"],
            domain=row["domain"],
            name=row["name"],
            key_prefix=row["key_prefix"],
            scopes=scopes,
        ),
        None,
    )


def list_api_keys(conn, domain=None):
    if domain:
        rows = conn.execute(
            """
            select id, domain, name, key_prefix, scopes_json, created_at, last_used_at, revoked_at
            from api_keys
            where domain = ?
            order by id
            """,
            (domain,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            select id, domain, name, key_prefix, scopes_json, created_at, last_used_at, revoked_at
            from api_keys
            order by id
            """
        ).fetchall()

    return [
        {
            "id": row["id"],
            "domain": row["domain"],
            "name": row["name"],
            "key_prefix": row["key_prefix"],
            "scopes": json.loads(row["scopes_json"]),
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "revoked_at": row["revoked_at"],
        }
        for row in rows
    ]


def revoke_api_key(conn, key_prefix_or_id):
    now = utc_now()
    with conn:
        if str(key_prefix_or_id).isdigit():
            conn.execute(
                "update api_keys set revoked_at = coalesce(revoked_at, ?) where id = ?",
                (now, int(key_prefix_or_id)),
            )
        else:
            conn.execute(
                "update api_keys set revoked_at = coalesce(revoked_at, ?) where key_prefix = ?",
                (now, key_prefix_or_id),
            )


def rotate_api_key(conn, key_prefix_or_id, name=None):
    selector_is_id = str(key_prefix_or_id).isdigit()
    if str(key_prefix_or_id).isdigit():
        row = conn.execute(
            "select id, domain, name, scopes_json from api_keys where id = ?",
            (int(key_prefix_or_id),),
        ).fetchone()
    else:
        row = conn.execute(
            "select id, domain, name, scopes_json from api_keys where key_prefix = ?",
            (key_prefix_or_id,),
        ).fetchone()

    if row is None:
        raise ValueError("key not found")

    domain, new_name, scopes = _normalize_key_input(
        row["domain"],
        name or row["name"],
        json.loads(row["scopes_json"]),
    )
    now = utc_now()

    for _ in range(8):
        full_key, key_prefix, key_hash = _new_key_material(domain)
        try:
            with conn:
                cursor = conn.execute(
                    """
                    insert into api_keys(domain, name, key_hash, key_prefix, scopes_json, created_at)
                    values (?, ?, ?, ?, ?, ?)
                    """,
                    (domain, new_name, key_hash, key_prefix, json.dumps(scopes), now),
                )
                if selector_is_id:
                    conn.execute(
                        "update api_keys set revoked_at = coalesce(revoked_at, ?) where id = ?",
                        (now, row["id"]),
                    )
                else:
                    conn.execute(
                        "update api_keys set revoked_at = coalesce(revoked_at, ?) where key_prefix = ?",
                        (now, key_prefix_or_id),
                    )
            return {
                "id": cursor.lastrowid,
                "key": full_key,
                "key_prefix": key_prefix,
                "domain": domain,
                "name": new_name,
                "scopes": scopes,
                "created_at": now,
            }
        except sqlite3.IntegrityError:
            continue

    raise RuntimeError("could not allocate unique api key prefix")

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

try:
    import fcntl
except ImportError:  # pragma: no cover - fcntl is unavailable on Windows.
    fcntl = None


MIGRATIONS = [
    (
        1,
        """
        create table if not exists api_keys (
            id integer primary key,
            domain text not null,
            name text not null,
            key_hash text not null,
            key_prefix text not null unique,
            scopes_json text not null,
            created_at text not null,
            last_used_at text null,
            revoked_at text null
        );
        """,
    ),
    (
        2,
        """
        drop table if exists gist_revisions;
        drop table if exists gists;

        create table gists (
            id integer primary key,
            external_id text not null unique,
            title text null,
            author_name text not null,
            markdown text not null,
            rendered_html text not null,
            render_version text not null,
            content_sha256 text not null,
            latest_revision_number integer not null,
            created_at text not null,
            updated_at text not null,
            deleted_at text null
        );

        create table gist_revisions (
            id integer primary key,
            gist_id integer not null references gists(id),
            revision_number integer not null,
            title text null,
            author_name text not null,
            markdown text not null,
            rendered_html text not null,
            render_version text not null,
            content_sha256 text not null,
            created_at text not null,
            created_by_key_id integer not null references api_keys(id)
        );

        create index if not exists idx_gist_revisions_gist_id
            on gist_revisions(gist_id);

        create unique index if not exists idx_gist_revisions_gist_id_revision_number
            on gist_revisions(gist_id, revision_number);
        """,
    ),
]


def get_gist_db_path(app=None):
    if app is not None:
        value = app.config.get("SQLITE_DB_PATH")
        if value:
            return value

    value = os.getenv("SQLITE_DB_PATH")
    if value:
        return value

    raise RuntimeError("SQLITE_DB_PATH must be set")


def _connect(db_path, busy_timeout_ms=5000):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute(f"pragma busy_timeout = {int(busy_timeout_ms)}")
    if db_path != ":memory:":
        conn.execute("pragma journal_mode = wal")
    return conn


@contextmanager
def gist_connection(app=None):
    db_path = get_gist_db_path(app)
    busy_timeout_ms = 5000
    if app is not None:
        busy_timeout_ms = app.config.get("SQLITE_BUSY_TIMEOUT_MS", 5000)

    conn = _connect(db_path, busy_timeout_ms)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _init_lock(db_path):
    if db_path == ":memory:" or fcntl is None:
        yield
        return

    lock_path = f"{db_path}.init.lock"
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def init_gist_database(app):
    db_path = get_gist_db_path(app)
    if db_path != ":memory:":
        Path(db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    with _init_lock(db_path):
        with gist_connection(app) as conn:
            conn.execute(
                """
                create table if not exists gist_schema_migrations (
                    version integer primary key,
                    applied_at text not null default (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                )
                """
            )
            applied = {
                row["version"]
                for row in conn.execute("select version from gist_schema_migrations")
            }

            for version, sql in MIGRATIONS:
                if version in applied:
                    continue
                with conn:
                    conn.executescript(sql)
                    conn.execute(
                        "insert into gist_schema_migrations(version) values (?)",
                        (version,),
                    )

import base64
import hashlib
import re
import secrets
import sqlite3

from .auth import utc_now
from .db import gist_connection
from .markdown import render_markdown, render_version


ID_RE = re.compile(r"^[A-Za-z0-9_-]{32}$")
SHA_RE = re.compile(r"^[a-f0-9]{64}$")


class GistError(Exception):
    def __init__(self, code, message, status):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def public_url(app, external_id):
    base_url = app.config["PUBLIC_GIST_BASE_URL"].rstrip("/")
    return f"{base_url}/{external_id}"


def normalize_markdown(value):
    if not isinstance(value, str):
        raise GistError("invalid_request", "markdown must be a string", 400)
    return value.replace("\r\n", "\n").replace("\r", "\n")


def normalize_title(value, *, present=True):
    if not present or value is None:
        return None
    if not isinstance(value, str):
        raise GistError("invalid_request", "title must be a string or null", 400)
    title = value.strip()
    if not title:
        return None
    if len(title) > 200:
        raise GistError("invalid_request", "title is too long", 400)
    return title


def validate_markdown(app, markdown):
    max_bytes = app.config.get("MAX_MARKDOWN_BYTES", 1048576)
    if len(markdown.encode("utf-8")) > max_bytes:
        raise GistError("payload_too_large", "Payload too large", 413)
    if not app.config.get("ALLOW_EMPTY_MARKDOWN", False) and not markdown.strip():
        raise GistError("invalid_request", "markdown is required", 400)


def content_sha256(markdown):
    return hashlib.sha256(markdown.encode("utf-8")).hexdigest()


def generate_external_id():
    return base64.urlsafe_b64encode(secrets.token_bytes(24)).rstrip(b"=").decode(
        "ascii"
    )


def validate_external_id(external_id):
    return isinstance(external_id, str) and bool(ID_RE.fullmatch(external_id))


def _row_to_api(app, row, *, include_markdown=False):
    body = {
        "id": row["external_id"],
        "url": public_url(app, row["external_id"]),
        "title": row["title"],
        "content_sha256": row["content_sha256"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if include_markdown:
        body["markdown"] = row["markdown"]
    return body


def _insert_revision(conn, gist_id, title, markdown, rendered_html, version, digest, key_id, created_at):
    conn.execute(
        """
        insert into gist_revisions(
            gist_id, title, markdown, rendered_html, render_version,
            content_sha256, created_at, created_by_key_id
        )
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (gist_id, title, markdown, rendered_html, version, digest, created_at, key_id),
    )


def create_gist(app, key_id, payload):
    markdown = normalize_markdown(payload.get("markdown"))
    validate_markdown(app, markdown)
    title = normalize_title(payload.get("title"), present="title" in payload)
    rendered_html = render_markdown(markdown)
    version = render_version()
    digest = content_sha256(markdown)
    now = utc_now()

    with gist_connection(app) as conn:
        for _ in range(8):
            external_id = generate_external_id()
            try:
                with conn:
                    cursor = conn.execute(
                        """
                        insert into gists(
                            external_id, title, markdown, rendered_html, render_version,
                            content_sha256, created_at, updated_at
                        )
                        values (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            external_id,
                            title,
                            markdown,
                            rendered_html,
                            version,
                            digest,
                            now,
                            now,
                        ),
                    )
                    gist_id = cursor.lastrowid
                    _insert_revision(
                        conn,
                        gist_id,
                        title,
                        markdown,
                        rendered_html,
                        version,
                        digest,
                        key_id,
                        now,
                    )
                row = conn.execute(
                    "select * from gists where id = ?",
                    (gist_id,),
                ).fetchone()
                return _row_to_api(app, row)
            except sqlite3.IntegrityError:
                continue

    raise GistError("internal_error", "Internal error", 500)


def get_gist(app, external_id, *, include_markdown=False):
    if not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)

    with gist_connection(app) as conn:
        row = conn.execute(
            "select * from gists where external_id = ? and deleted_at is null",
            (external_id,),
        ).fetchone()
        if row is None:
            raise GistError("not_found", "Not found", 404)
        return _row_to_api(app, row, include_markdown=include_markdown)


def get_public_render(app, external_id):
    if not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)

    with gist_connection(app) as conn:
        row = conn.execute(
            """
            select external_id, title, markdown, rendered_html, updated_at
            from gists
            where external_id = ? and deleted_at is null
            """,
            (external_id,),
        ).fetchone()
        if row is None:
            raise GistError("not_found", "Not found", 404)
        return {
            "id": row["external_id"],
            "title": row["title"],
            "markdown": row["markdown"],
            "rendered_html": row["rendered_html"],
            "updated_at": row["updated_at"],
        }


def patch_gist(app, key_id, external_id, payload):
    if not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)
    if "markdown" not in payload and "title" not in payload:
        raise GistError("invalid_request", "markdown or title is required", 400)

    expected_digest = payload.get("expected_content_sha256")
    if expected_digest is not None and not (
        isinstance(expected_digest, str) and SHA_RE.fullmatch(expected_digest)
    ):
        raise GistError("invalid_request", "expected_content_sha256 is invalid", 400)

    with gist_connection(app) as conn:
        current = conn.execute(
            "select * from gists where external_id = ? and deleted_at is null",
            (external_id,),
        ).fetchone()
        if current is None:
            raise GistError("not_found", "Not found", 404)
        if expected_digest is not None and expected_digest != current["content_sha256"]:
            raise GistError("conflict", "Conflict", 409)

    if "markdown" in payload:
        markdown = normalize_markdown(payload["markdown"])
        validate_markdown(app, markdown)
        rendered_html = render_markdown(markdown)
        version = render_version()
        digest = content_sha256(markdown)
    else:
        markdown = current["markdown"]
        rendered_html = current["rendered_html"]
        version = current["render_version"]
        digest = current["content_sha256"]

    title = (
        normalize_title(payload.get("title"), present=True)
        if "title" in payload
        else current["title"]
    )
    now = utc_now()

    with gist_connection(app) as conn:
        with conn:
            current = conn.execute(
                "select * from gists where external_id = ? and deleted_at is null",
                (external_id,),
            ).fetchone()
            if current is None:
                raise GistError("not_found", "Not found", 404)
            if expected_digest is not None and expected_digest != current["content_sha256"]:
                raise GistError("conflict", "Conflict", 409)

            conn.execute(
                """
                update gists
                set title = ?, markdown = ?, rendered_html = ?, render_version = ?,
                    content_sha256 = ?, updated_at = ?
                where id = ?
                """,
                (
                    title,
                    markdown,
                    rendered_html,
                    version,
                    digest,
                    now,
                    current["id"],
                ),
            )
            _insert_revision(
                conn,
                current["id"],
                title,
                markdown,
                rendered_html,
                version,
                digest,
                key_id,
                now,
            )

        row = conn.execute("select * from gists where id = ?", (current["id"],)).fetchone()
        return _row_to_api(app, row)


def delete_gist(app, external_id):
    if not validate_external_id(external_id):
        return

    now = utc_now()
    with gist_connection(app) as conn:
        with conn:
            conn.execute(
                "update gists set deleted_at = coalesce(deleted_at, ?) where external_id = ?",
                (now, external_id),
            )

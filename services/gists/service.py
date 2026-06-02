import hashlib
import re
import secrets
import sqlite3

from .auth import utc_now
from .db import gist_connection
from .markdown import render_markdown_result, render_version


ID_RE = re.compile(r"^(?:[A-Za-z0-9]{16}|[A-Za-z0-9_-]{32})$")
EXTERNAL_ID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
REVISION_RE = re.compile(r"^[1-9][0-9]*$")
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


def revision_url(app, external_id, revision_number):
    return f"{public_url(app, external_id)}/revisions/{revision_number}"


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


def normalize_author_name(value):
    author_name = (value or "").strip()
    if not author_name:
        raise GistError("invalid_request", "API key name is required", 400)
    return author_name


def validate_markdown(app, markdown):
    max_bytes = app.config.get("MAX_MARKDOWN_BYTES", 1048576)
    if len(markdown.encode("utf-8")) > max_bytes:
        raise GistError("payload_too_large", "Payload too large", 413)
    if not app.config.get("ALLOW_EMPTY_MARKDOWN", False) and not markdown.strip():
        raise GistError("invalid_request", "markdown is required", 400)


def content_sha256(markdown):
    return hashlib.sha256(markdown.encode("utf-8")).hexdigest()


def generate_external_id():
    return "".join(secrets.choice(EXTERNAL_ID_ALPHABET) for _ in range(16))


def validate_external_id(external_id):
    return isinstance(external_id, str) and bool(ID_RE.fullmatch(external_id))


def parse_revision_number(revision_number):
    if isinstance(revision_number, int):
        if revision_number > 0:
            return revision_number
        raise GistError("not_found", "Not found", 404)
    if not isinstance(revision_number, str) or not REVISION_RE.fullmatch(revision_number):
        raise GistError("not_found", "Not found", 404)
    return int(revision_number)


def _row_to_api(app, row, *, include_markdown=False):
    latest_revision_number = row["latest_revision_number"]
    body = {
        "id": row["external_id"],
        "url": public_url(app, row["external_id"]),
        "title": row["title"],
        "author_name": row["author_name"],
        "content_sha256": row["content_sha256"],
        "revision_number": latest_revision_number,
        "latest_revision_number": latest_revision_number,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if include_markdown:
        body["markdown"] = row["markdown"]
    return body


def _insert_revision(
    conn,
    gist_id,
    revision_number,
    title,
    author_name,
    markdown,
    rendered_html,
    version,
    digest,
    key_id,
    created_at,
):
    conn.execute(
        """
        insert into gist_revisions(
            gist_id, revision_number, title, author_name, markdown,
            rendered_html, render_version,
            content_sha256, created_at, created_by_key_id
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            gist_id,
            revision_number,
            title,
            author_name,
            markdown,
            rendered_html,
            version,
            digest,
            created_at,
            key_id,
        ),
    )


def create_gist(app, key_id, author_name, payload):
    author_name = normalize_author_name(author_name)
    markdown = normalize_markdown(payload.get("markdown"))
    validate_markdown(app, markdown)
    title = normalize_title(payload.get("title"), present="title" in payload)
    rendered = render_markdown_result(markdown)
    rendered_html = rendered.html
    version = rendered.version
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
                            external_id, title, author_name, markdown, rendered_html,
                            render_version, content_sha256, latest_revision_number,
                            created_at, updated_at
                        )
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            external_id,
                            title,
                            author_name,
                            markdown,
                            rendered_html,
                            version,
                            digest,
                            1,
                            now,
                            now,
                        ),
                    )
                    gist_id = cursor.lastrowid
                    _insert_revision(
                        conn,
                        gist_id,
                        1,
                        title,
                        author_name,
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


def _history_payload(app, conn, external_id, gist_id, latest_revision_number):
    rows = conn.execute(
        """
        select revision_number, created_at, author_name
        from gist_revisions
        where gist_id = ?
        order by revision_number desc
        limit 50
        """,
        (gist_id,),
    ).fetchall()

    return [
        {
            "revision_number": row["revision_number"],
            "created_at": row["created_at"],
            "author_name": row["author_name"],
            "is_latest": row["revision_number"] == latest_revision_number,
            "url": (
                public_url(app, external_id)
                if row["revision_number"] == latest_revision_number
                else revision_url(app, external_id, row["revision_number"])
            ),
        }
        for row in rows
    ]


def get_public_render(app, external_id, revision_number=None):
    if not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)
    parsed_revision_number = (
        parse_revision_number(revision_number) if revision_number is not None else None
    )

    with gist_connection(app) as conn:
        if parsed_revision_number is None:
            row = conn.execute(
                """
                select id, external_id, title, author_name, markdown, rendered_html,
                       latest_revision_number, updated_at
                from gists
                where external_id = ? and deleted_at is null
                """,
                (external_id,),
            ).fetchone()
            if row is None:
                raise GistError("not_found", "Not found", 404)
            revision_number = row["latest_revision_number"]
        else:
            row = conn.execute(
                """
                select gists.id, gists.external_id, gists.latest_revision_number,
                       gist_revisions.title, gist_revisions.author_name,
                       gist_revisions.markdown, gist_revisions.rendered_html,
                       gist_revisions.created_at as updated_at,
                       gist_revisions.revision_number
                from gists
                join gist_revisions on gist_revisions.gist_id = gists.id
                where gists.external_id = ?
                  and gists.deleted_at is null
                  and gist_revisions.revision_number = ?
                """,
                (external_id, parsed_revision_number),
            ).fetchone()
            if row is None:
                raise GistError("not_found", "Not found", 404)
            revision_number = row["revision_number"]

        return {
            "id": row["external_id"],
            "title": row["title"],
            "author_name": row["author_name"],
            "markdown": row["markdown"],
            "rendered_html": row["rendered_html"],
            "revision_number": revision_number,
            "latest_revision_number": row["latest_revision_number"],
            "updated_at": row["updated_at"],
            "history": _history_payload(
                app,
                conn,
                row["external_id"],
                row["id"],
                row["latest_revision_number"],
            ),
        }


def patch_gist(app, key_id, author_name, external_id, payload):
    if not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)
    if "markdown" not in payload and "title" not in payload:
        raise GistError("invalid_request", "markdown or title is required", 400)
    author_name = normalize_author_name(author_name)

    expected_digest = payload.get("expected_content_sha256")
    if expected_digest is not None and not (
        isinstance(expected_digest, str) and SHA_RE.fullmatch(expected_digest)
    ):
        raise GistError("invalid_request", "expected_content_sha256 is invalid", 400)

    if "markdown" in payload:
        markdown = normalize_markdown(payload["markdown"])
        validate_markdown(app, markdown)
        rendered = render_markdown_result(markdown)
        rendered_html = rendered.html
        version = rendered.version
        digest = content_sha256(markdown)
    else:
        markdown = None
        rendered_html = None
        version = None
        digest = None

    title = (
        normalize_title(payload.get("title"), present=True)
        if "title" in payload
        else None
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
            if (
                expected_digest is not None
                and expected_digest != current["content_sha256"]
            ):
                raise GistError("conflict", "Conflict", 409)

            next_title = title if "title" in payload else current["title"]
            next_markdown = markdown if markdown is not None else current["markdown"]
            next_rendered_html = (
                rendered_html if rendered_html is not None else current["rendered_html"]
            )
            next_version = version if version is not None else current["render_version"]
            next_digest = digest if digest is not None else current["content_sha256"]
            next_revision_number = current["latest_revision_number"] + 1

            conn.execute(
                """
                update gists
                set title = ?, author_name = ?, markdown = ?, rendered_html = ?,
                    render_version = ?, content_sha256 = ?,
                    latest_revision_number = ?, updated_at = ?
                where id = ?
                """,
                (
                    next_title,
                    author_name,
                    next_markdown,
                    next_rendered_html,
                    next_version,
                    next_digest,
                    next_revision_number,
                    now,
                    current["id"],
                ),
            )
            _insert_revision(
                conn,
                current["id"],
                next_revision_number,
                next_title,
                author_name,
                next_markdown,
                next_rendered_html,
                next_version,
                next_digest,
                key_id,
                now,
            )

        row = conn.execute(
            "select * from gists where id = ?",
            (current["id"],),
        ).fetchone()
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


def rerender_gists(app, *, external_id=None, dry_run=False):
    if external_id is not None and not validate_external_id(external_id):
        raise GistError("not_found", "Not found", 404)

    with gist_connection(app) as conn:
        if external_id is None:
            gists = conn.execute("select id, markdown from gists").fetchall()
            revisions = conn.execute(
                "select id, markdown from gist_revisions"
            ).fetchall()
        else:
            gists = conn.execute(
                "select id, markdown from gists where external_id = ?",
                (external_id,),
            ).fetchall()
            if not gists:
                raise GistError("not_found", "Not found", 404)
            revisions = conn.execute(
                """
                select gist_revisions.id, gist_revisions.markdown
                from gist_revisions
                join gists on gists.id = gist_revisions.gist_id
                where gists.external_id = ?
                """,
                (external_id,),
            ).fetchall()

        rendered_gists = []
        for row in gists:
            rendered = render_markdown_result(row["markdown"])
            rendered_gists.append((row["id"], rendered.html, rendered.version))

        rendered_revisions = []
        for row in revisions:
            rendered = render_markdown_result(row["markdown"])
            rendered_revisions.append((row["id"], rendered.html, rendered.version))

        if not dry_run:
            with conn:
                conn.executemany(
                    """
                    update gists
                    set rendered_html = ?, render_version = ?
                    where id = ?
                    """,
                    [
                        (rendered_html, version, row_id)
                        for row_id, rendered_html, version in rendered_gists
                    ],
                )
                conn.executemany(
                    """
                    update gist_revisions
                    set rendered_html = ?, render_version = ?
                    where id = ?
                    """,
                    [
                        (rendered_html, version, row_id)
                        for row_id, rendered_html, version in rendered_revisions
                    ],
                )

    return {
        "dry_run": dry_run,
        "gists": len(rendered_gists),
        "revisions": len(rendered_revisions),
        "render_version": render_version(),
    }

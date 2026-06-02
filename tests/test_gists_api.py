from pathlib import Path

import pytest
from flask import Flask, jsonify

from services.gists.auth import create_api_key, rotate_api_key, verify_api_key
from services.gists.db import gist_connection, init_gist_database
from services.gists.markdown import render_markdown, render_version
from services.gists.routes import gists_api
from services.gists.service import rerender_gists


FIXTURE_DIR = Path(__file__).with_name("fixtures")


@pytest.fixture()
def app(tmp_path):
    app = Flask(__name__)
    app.config.update(
        SQLITE_DB_PATH=str(tmp_path / "gists.sqlite3"),
        PUBLIC_GIST_BASE_URL="https://gist.example.com",
        MAX_MARKDOWN_BYTES=1024 * 1024,
        ALLOW_EMPTY_MARKDOWN=False,
        API_RATE_LIMIT_PER_MINUTE=1000,
        API_AUTH_FAILURE_LIMIT_PER_MINUTE=1000,
    )
    app.register_blueprint(gists_api)

    @app.route("/api/v1/other")
    def other():
        return jsonify({"ok": True})

    init_gist_database(app)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def make_key(app, scopes, domain="gist", name="test"):
    with gist_connection(app) as conn:
        return create_api_key(conn, domain, name, scopes)["key"]


def auth_header(key):
    return {"Authorization": f"Bearer {key}"}


def create_gist(client, key, markdown="# Hello", title="Title"):
    return client.post(
        "/api/v1/gists",
        headers=auth_header(key),
        json={"title": title, "markdown": markdown},
    )


def test_markdown_rendering_uses_gfm_highlighting_links_and_sanitizer():
    markdown = (FIXTURE_DIR / "github_like_gist.md").read_text(encoding="utf-8")
    html = render_markdown(markdown)

    assert "<table>" in html
    assert html.count("<table>") == 2
    assert "<th>Field</th>" in html
    assert "highlight highlight-source-solidity" in html
    assert html.count("highlight highlight-source-solidity") == 2
    assert "highlight highlight-source-vyper" in html
    assert "highlight highlight-source-go" in html
    assert "highlight highlight-source-python" in html
    assert "highlight highlight-source-shell" in html
    assert "highlight highlight-source-json" in html
    assert "highlight highlight-source-yaml" in html
    assert "highlight highlight-source-ts" in html
    assert "highlight highlight-text-md" in html
    assert "highlight highlight-source-sql" in html
    assert "highlight highlight-source-rust" in html
    assert "highlight highlight-source-diff" in html
    assert "class=\"pl-" in html
    assert '<input type="checkbox" checked disabled>' in html
    assert '<input type="checkbox" disabled>' in html
    assert "<del>deprecated</del>" in html
    assert '<code class="language-unknownlang">&lt;tag onclick="bad()"&gt;' in html
    assert "<code>indented code remains plain\n</code>" in html
    assert '<a href="https://example.com" rel="nofollow">https://example.com</a>' in html
    assert "javascript:" not in html
    assert "<a>bad</a>" in html
    assert "alert(" not in html
    assert "<script" not in html
    assert "<style" not in html
    assert "<svg" not in html
    assert "<math" not in html
    assert "<iframe" not in html
    assert 'class="bad"' not in html
    assert 'class="pl-c bad"' not in html
    assert "cmarkgfm/" in render_version()
    assert "starry-night/" in render_version()
    assert "syntax-css/" in render_version()


def test_rerender_gists_updates_current_rows_and_revisions(client, app):
    write_key = make_key(app, ["gist:write"], name="renderer")
    created = create_gist(
        client,
        write_key,
        markdown="```python\nprint('hi')\n```",
    )
    assert created.status_code == 201
    gist_id = created.get_json()["id"]

    with gist_connection(app) as conn:
        with conn:
            conn.execute(
                """
                update gists
                set rendered_html = 'old', render_version = 'old'
                where external_id = ?
                """,
                (gist_id,),
            )
            conn.execute(
                """
                update gist_revisions
                set rendered_html = 'old', render_version = 'old'
                where gist_id = (select id from gists where external_id = ?)
                """,
                (gist_id,),
            )

    dry_run = rerender_gists(app, external_id=gist_id, dry_run=True)
    assert dry_run["dry_run"] is True
    assert dry_run["gists"] == 1
    assert dry_run["revisions"] == 1

    with gist_connection(app) as conn:
        row = conn.execute(
            "select rendered_html, render_version from gists where external_id = ?",
            (gist_id,),
        ).fetchone()
        assert dict(row) == {"rendered_html": "old", "render_version": "old"}

    result = rerender_gists(app, external_id=gist_id)
    assert result["dry_run"] is False
    assert result["gists"] == 1
    assert result["revisions"] == 1

    with gist_connection(app) as conn:
        row = conn.execute(
            "select rendered_html, render_version from gists where external_id = ?",
            (gist_id,),
        ).fetchone()
        revision = conn.execute(
            """
            select rendered_html, render_version
            from gist_revisions
            where gist_id = (select id from gists where external_id = ?)
            """,
            (gist_id,),
        ).fetchone()

    assert "highlight highlight-source-python" in row["rendered_html"]
    assert "cmarkgfm/" in row["render_version"]
    assert "highlight highlight-source-python" in revision["rendered_html"]
    assert "cmarkgfm/" in revision["render_version"]


def test_create_public_render_raw_read_patch_and_delete(client, app):
    write_key = make_key(app, ["gist:write", "gist:delete"], name="creator")
    read_key = make_key(app, ["gist:read"])

    denied = client.post("/api/v1/gists", json={"markdown": "# Nope"})
    assert denied.status_code == 401

    created = client.post(
        "/api/v1/gists",
        headers=auth_header(write_key),
        json={
            "title": "Title",
            "markdown": "# Hello\n\n- [x] done",
            "author_name": "spoofed",
        },
    )
    assert created.status_code == 201
    body = created.get_json()
    assert set(body) == {
        "id",
        "url",
        "title",
        "author_name",
        "content_sha256",
        "revision_number",
        "latest_revision_number",
        "created_at",
        "updated_at",
    }
    assert len(body["id"]) == 32
    assert body["url"] == f"https://gist.example.com/{body['id']}"
    assert body["author_name"] == "creator"
    assert body["revision_number"] == 1
    assert body["latest_revision_number"] == 1

    public = client.get(f"/api/v1/gists/{body['id']}/render")
    assert public.status_code == 200
    public_body = public.get_json()
    assert public_body["markdown"] == "# Hello\n\n- [x] done"
    assert public_body["author_name"] == "creator"
    assert public_body["revision_number"] == 1
    assert public_body["latest_revision_number"] == 1
    assert "<h1>Hello</h1>" in public_body["rendered_html"]
    assert "disabled" in public_body["rendered_html"]
    assert "url" not in public_body
    assert public_body["history"] == [
        {
            "revision_number": 1,
            "created_at": public_body["history"][0]["created_at"],
            "author_name": "creator",
            "is_latest": True,
            "url": body["url"],
        }
    ]

    forbidden = client.get(f"/api/v1/gists/{body['id']}", headers=auth_header(write_key))
    assert forbidden.status_code == 403

    raw = client.get(f"/api/v1/gists/{body['id']}", headers=auth_header(read_key))
    assert raw.status_code == 200
    raw_body = raw.get_json()
    assert raw_body["markdown"] == "# Hello\n\n- [x] done"
    assert raw_body["author_name"] == "creator"

    stale = client.patch(
        f"/api/v1/gists/{body['id']}",
        headers=auth_header(write_key),
        json={"markdown": "# Stale", "expected_content_sha256": "a" * 64},
    )
    assert stale.status_code == 409

    editor_key = make_key(app, ["gist:write", "gist:delete"], name="editor")
    updated = client.patch(
        f"/api/v1/gists/{body['id']}",
        headers=auth_header(editor_key),
        json={"title": None, "markdown": "# Updated"},
    )
    assert updated.status_code == 200
    updated_body = updated.get_json()
    assert updated_body["title"] is None
    assert updated_body["author_name"] == "editor"
    assert updated_body["revision_number"] == 2
    assert updated_body["latest_revision_number"] == 2

    with gist_connection(app) as conn:
        revision_rows = conn.execute(
            """
            select gist_revisions.revision_number, gist_revisions.author_name
            from gist_revisions
            join gists on gists.id = gist_revisions.gist_id
            where gists.external_id = ?
            order by revision_number
            """,
            (body["id"],),
        ).fetchall()
        gist_row = conn.execute(
            "select author_name, latest_revision_number from gists where external_id = ?",
            (body["id"],),
        ).fetchone()
    assert [dict(row) for row in revision_rows] == [
        {"revision_number": 1, "author_name": "creator"},
        {"revision_number": 2, "author_name": "editor"},
    ]
    assert dict(gist_row) == {"author_name": "editor", "latest_revision_number": 2}

    latest = client.get(f"/api/v1/gists/{body['id']}/render")
    assert latest.status_code == 200
    latest_body = latest.get_json()
    assert latest_body["markdown"] == "# Updated"
    assert latest_body["author_name"] == "editor"
    assert latest_body["revision_number"] == 2
    assert latest_body["latest_revision_number"] == 2
    assert len(latest_body["history"]) == 2
    assert latest_body["history"][0]["revision_number"] == 2
    assert latest_body["history"][0]["is_latest"] is True
    assert latest_body["history"][0]["url"] == body["url"]
    assert latest_body["history"][1]["revision_number"] == 1
    assert latest_body["history"][1]["is_latest"] is False
    assert latest_body["history"][1]["url"] == f"{body['url']}/revisions/1"

    first_revision = client.get(f"/api/v1/gists/{body['id']}/revisions/1/render")
    assert first_revision.status_code == 200
    first_revision_body = first_revision.get_json()
    assert first_revision_body["markdown"] == "# Hello\n\n- [x] done"
    assert first_revision_body["author_name"] == "creator"
    assert first_revision_body["revision_number"] == 1
    assert first_revision_body["latest_revision_number"] == 2

    assert client.get(f"/api/v1/gists/{body['id']}/revisions/0/render").status_code == 404
    assert client.get(f"/api/v1/gists/{body['id']}/revisions/nope/render").status_code == 404

    deleted = client.delete(f"/api/v1/gists/{body['id']}", headers=auth_header(editor_key))
    assert deleted.status_code == 204

    assert client.get(f"/api/v1/gists/{body['id']}/render").status_code == 404
    assert client.get(f"/api/v1/gists/{body['id']}/revisions/1/render").status_code == 404
    assert client.get(f"/api/v1/gists/{body['id']}", headers=auth_header(read_key)).status_code == 404


def test_sanitizer_strips_scriptable_content(client, app):
    write_key = make_key(app, ["gist:write"])
    markdown = """
<script>alert(1)</script>
<img src="javascript:alert(1)" onerror="alert(1)">
<a href="javascript:alert(1)" onclick="alert(1)">bad</a>
<svg><script>alert(1)</script></svg>
"""

    created = create_gist(client, write_key, markdown)
    assert created.status_code == 201
    gist_id = created.get_json()["id"]

    public = client.get(f"/api/v1/gists/{gist_id}/render").get_json()
    html = public["rendered_html"].lower()
    assert "<script" not in html
    assert "alert(1)" not in html
    assert "javascript:" not in html
    assert "onerror" not in html
    assert "onclick" not in html
    assert "<svg" not in html


def test_public_history_is_bounded_to_latest_50_revisions(client, app):
    write_key = make_key(app, ["gist:write"], name="historian")
    created = create_gist(client, write_key, "# First")
    assert created.status_code == 201
    gist_id = created.get_json()["id"]

    for index in range(52):
        updated = client.patch(
            f"/api/v1/gists/{gist_id}",
            headers=auth_header(write_key),
            json={"title": f"Revision {index + 2}"},
        )
        assert updated.status_code == 200

    public = client.get(f"/api/v1/gists/{gist_id}/render")
    assert public.status_code == 200
    history = public.get_json()["history"]
    assert len(history) == 50
    assert history[0]["revision_number"] == 53
    assert history[0]["is_latest"] is True
    assert history[-1]["revision_number"] == 4

    first_revision = client.get(f"/api/v1/gists/{gist_id}/revisions/1/render")
    assert first_revision.status_code == 200
    assert first_revision.get_json()["revision_number"] == 1


def test_validation_and_non_gist_routes_are_not_globally_authed(client, app):
    key = make_key(app, ["gist:write"])

    assert client.get("/api/v1/other").status_code == 200
    assert client.get("/api/v1/gists/not-a-valid-id/render").status_code == 404

    empty = create_gist(client, key, "   ")
    assert empty.status_code == 400

    long_title = client.post(
        "/api/v1/gists",
        headers=auth_header(key),
        json={"title": "x" * 201, "markdown": "ok"},
    )
    assert long_title.status_code == 400


def test_key_rotation_revokes_old_key_and_returns_new_secret(app):
    with gist_connection(app) as conn:
        created = create_api_key(conn, "gist", "rotate", ["gist:read"])
        rotated = rotate_api_key(conn, created["key_prefix"], "rotated")

        old_auth, old_error = verify_api_key(
            conn,
            f"Bearer {created['key']}",
            "gist",
            "gist:read",
        )
        new_auth, new_error = verify_api_key(
            conn,
            f"Bearer {rotated['key']}",
            "gist",
            "gist:read",
        )

    assert old_auth is None
    assert old_error == "unauthorized"
    assert new_auth is not None
    assert new_error is None
    assert rotated["key"].startswith("wapi_gist_")

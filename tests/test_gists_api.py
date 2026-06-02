import pytest
from flask import Flask, jsonify

from services.gists.auth import create_api_key, rotate_api_key, verify_api_key
from services.gists.db import gist_connection, init_gist_database
from services.gists.routes import gists_api


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


def make_key(app, scopes, domain="gist"):
    with gist_connection(app) as conn:
        return create_api_key(conn, domain, "test", scopes)["key"]


def auth_header(key):
    return {"Authorization": f"Bearer {key}"}


def create_gist(client, key, markdown="# Hello", title="Title"):
    return client.post(
        "/api/v1/gists",
        headers=auth_header(key),
        json={"title": title, "markdown": markdown},
    )


def test_create_public_render_raw_read_patch_and_delete(client, app):
    write_key = make_key(app, ["gist:write", "gist:delete"])
    read_key = make_key(app, ["gist:read"])

    denied = client.post("/api/v1/gists", json={"markdown": "# Nope"})
    assert denied.status_code == 401

    created = create_gist(client, write_key, "# Hello\n\n- [x] done")
    assert created.status_code == 201
    body = created.get_json()
    assert set(body) == {
        "id",
        "url",
        "title",
        "content_sha256",
        "created_at",
        "updated_at",
    }
    assert len(body["id"]) == 32
    assert body["url"] == f"https://gist.example.com/{body['id']}"

    public = client.get(f"/api/v1/gists/{body['id']}/render")
    assert public.status_code == 200
    public_body = public.get_json()
    assert public_body["markdown"] == "# Hello\n\n- [x] done"
    assert "<h1>Hello</h1>" in public_body["rendered_html"]
    assert "disabled" in public_body["rendered_html"]
    assert "url" not in public_body

    forbidden = client.get(f"/api/v1/gists/{body['id']}", headers=auth_header(write_key))
    assert forbidden.status_code == 403

    raw = client.get(f"/api/v1/gists/{body['id']}", headers=auth_header(read_key))
    assert raw.status_code == 200
    assert raw.get_json()["markdown"] == "# Hello\n\n- [x] done"

    stale = client.patch(
        f"/api/v1/gists/{body['id']}",
        headers=auth_header(write_key),
        json={"markdown": "# Stale", "expected_content_sha256": "a" * 64},
    )
    assert stale.status_code == 409

    updated = client.patch(
        f"/api/v1/gists/{body['id']}",
        headers=auth_header(write_key),
        json={"title": None, "markdown": "# Updated"},
    )
    assert updated.status_code == 200
    assert updated.get_json()["title"] is None

    with gist_connection(app) as conn:
        revision_count = conn.execute(
            """
            select count(*) as count
            from gist_revisions
            join gists on gists.id = gist_revisions.gist_id
            where gists.external_id = ?
            """,
            (body["id"],),
        ).fetchone()["count"]
    assert revision_count == 2

    deleted = client.delete(f"/api/v1/gists/{body['id']}", headers=auth_header(write_key))
    assert deleted.status_code == 204

    assert client.get(f"/api/v1/gists/{body['id']}/render").status_code == 404
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

from collections import defaultdict, deque
from time import time

from flask import Blueprint, current_app, jsonify, request

from .auth import verify_api_key
from .db import gist_connection
from .service import (
    GistError,
    create_gist,
    delete_gist,
    get_gist,
    get_public_render,
    patch_gist,
)


gists_api = Blueprint("gists_api", __name__)
_rate_buckets = defaultdict(deque)


def error_response(code, message, status):
    return jsonify({"error": {"code": code, "message": message}}), status


def _client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.remote_addr or "unknown"


def _rate_limited(bucket_key, limit):
    now = time()
    bucket = _rate_buckets[bucket_key]
    while bucket and bucket[0] <= now - 60:
        bucket.popleft()
    if len(bucket) >= limit:
        return True
    bucket.append(now)
    return False


def _check_rate(bucket_key, limit):
    if _rate_limited(bucket_key, limit):
        return error_response("rate_limited", "Rate limited", 429)
    return None


def parse_json_body():
    max_bytes = current_app.config.get("MAX_MARKDOWN_BYTES", 1048576)
    if request.content_length is not None and request.content_length > max_bytes + 2048:
        raise GistError("payload_too_large", "Payload too large", 413)
    if not request.is_json:
        raise GistError("invalid_request", "JSON body required", 400)
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise GistError("invalid_request", "JSON object required", 400)
    return data


def require_gist_auth(scope):
    with gist_connection(current_app) as conn:
        auth, error_code = verify_api_key(
            conn,
            request.headers.get("Authorization"),
            "gist",
            scope,
        )

    if error_code == "unauthorized":
        failure_key = ("auth-failure", _client_ip())
        if _rate_limited(
            failure_key,
            current_app.config.get("API_AUTH_FAILURE_LIMIT_PER_MINUTE", 20),
        ):
            return None, error_response("rate_limited", "Rate limited", 429)
        return None, error_response("unauthorized", "Unauthorized", 401)
    if error_code == "forbidden":
        return None, error_response("forbidden", "Forbidden", 403)
    return auth, None


@gists_api.route("/api/v1/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True})


@gists_api.route("/api/v1/gists", methods=["POST"])
def post_gist():
    auth, response = require_gist_auth("gist:write")
    if response:
        return response
    response = _check_rate(
        ("write", auth.key_prefix, _client_ip()),
        current_app.config.get("API_RATE_LIMIT_PER_MINUTE", 60),
    )
    if response:
        return response

    try:
        body = create_gist(current_app, auth.key_id, auth.name, parse_json_body())
        return jsonify(body), 201
    except GistError as error:
        return error_response(error.code, error.message, error.status)


@gists_api.route("/api/v1/gists/<gist_id>", methods=["GET"])
def read_gist(gist_id):
    auth, response = require_gist_auth("gist:read")
    if response:
        return response

    try:
        return jsonify(get_gist(current_app, gist_id, include_markdown=True))
    except GistError as error:
        return error_response(error.code, error.message, error.status)


@gists_api.route("/api/v1/gists/<gist_id>/render", methods=["GET"])
def render_gist(gist_id):
    try:
        return jsonify(get_public_render(current_app, gist_id))
    except GistError as error:
        return error_response(error.code, error.message, error.status)


@gists_api.route(
    "/api/v1/gists/<gist_id>/revisions/<revision_number>/render",
    methods=["GET"],
)
def render_gist_revision(gist_id, revision_number):
    try:
        return jsonify(get_public_render(current_app, gist_id, revision_number))
    except GistError as error:
        return error_response(error.code, error.message, error.status)


@gists_api.route("/api/v1/gists/<gist_id>", methods=["PATCH"])
def update_gist(gist_id):
    auth, response = require_gist_auth("gist:write")
    if response:
        return response
    response = _check_rate(
        ("write", auth.key_prefix, _client_ip()),
        current_app.config.get("API_RATE_LIMIT_PER_MINUTE", 60),
    )
    if response:
        return response

    try:
        body = patch_gist(
            current_app,
            auth.key_id,
            auth.name,
            gist_id,
            parse_json_body(),
        )
        return jsonify(body)
    except GistError as error:
        return error_response(error.code, error.message, error.status)


@gists_api.route("/api/v1/gists/<gist_id>", methods=["DELETE"])
def remove_gist(gist_id):
    auth, response = require_gist_auth("gist:delete")
    if response:
        return response
    response = _check_rate(
        ("write", auth.key_prefix, _client_ip()),
        current_app.config.get("API_RATE_LIMIT_PER_MINUTE", 60),
    )
    if response:
        return response

    delete_gist(current_app, gist_id)
    return "", 204

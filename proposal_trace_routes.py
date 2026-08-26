"""HTTP projection of the versioned Proposal Trace read-only contract."""

import hmac

from flask import Blueprint, current_app, jsonify, request

from services.proposal_trace import (
    DataUnavailable as ProposalTraceUnavailable,
    InvalidRequest as ProposalTraceInvalidRequest,
    NotFound as ProposalTraceNotFound,
    ProposalTraceService,
    parse_source_path,
)
from services.proposal_trace_policy import (
    AUTHENTICATED,
    DISABLED,
    exposure_policy,
)


proposal_trace_api = Blueprint("proposal_trace_api", __name__)


def _service():
    return ProposalTraceService(
        current_app.config.get("PROPOSAL_TRACE_DB_PATH"),
        current_app.config.get("PROPOSAL_TRACE_BUSY_TIMEOUT_MS", 5000),
    )


def _authorize(source_id):
    policy = exposure_policy(source_id)
    if policy == DISABLED:
        raise ProposalTraceNotFound("source not found")
    if policy == AUTHENTICATED:
        expected = current_app.config.get("PROPOSAL_TRACE_API_KEY")
        supplied = request.headers.get("X-Proposal-Trace-Key")
        if not expected:
            raise ProposalTraceUnavailable("Proposal Trace API key is not configured")
        if not supplied or not hmac.compare_digest(supplied, expected):
            return False, policy
    return True, policy


def _response(body, policy):
    response = jsonify(body)
    response.headers["Cache-Control"] = (
        "public, max-age=30" if policy != AUTHENTICATED else "private, no-store"
    )
    return response


def _error(error):
    if isinstance(error, ProposalTraceInvalidRequest):
        return jsonify({"error": "invalid_request"}), 400
    if isinstance(error, ProposalTraceNotFound):
        return jsonify({"error": "not_found"}), 404
    current_app.logger.error(
        "event=proposal_trace_unavailable error=%s", type(error).__name__
    )
    return jsonify({"error": "unavailable"}), 503


@proposal_trace_api.route(
    "/proposal-trace/safe/<chain_id>/<safe_address>/audits/by-nonce/<nonce>",
    methods=["GET"],
)
def get_safe_audit_by_nonce(chain_id, safe_address, nonce):
    try:
        source_id = parse_source_path("safe/{}/{}".format(chain_id, safe_address))
        authorized, policy = _authorize(source_id)
        if not authorized:
            return jsonify({"error": "unauthorized"}), 401
        selected_source, body = _service().safe_audit_by_nonce(
            chain_id, safe_address, nonce
        )
        if selected_source != source_id:
            raise ProposalTraceUnavailable("Proposal Trace source changed")
        return _response(body, policy)
    except (
        ProposalTraceInvalidRequest,
        ProposalTraceNotFound,
        ProposalTraceUnavailable,
    ) as error:
        return _error(error)


@proposal_trace_api.route("/proposal-trace/<path:source_path>/audits", methods=["GET"])
def list_audits(source_path):
    try:
        if set(request.args) - {"limit", "cursor"}:
            raise ProposalTraceInvalidRequest("unsupported query parameter")
        source_id = parse_source_path(source_path)
        authorized, policy = _authorize(source_id)
        if not authorized:
            return jsonify({"error": "unauthorized"}), 401
        body = _service().list_audits(
            source_id,
            request.args.get("limit"),
            request.args.get("cursor"),
        )
        return _response(body, policy)
    except (
        ProposalTraceInvalidRequest,
        ProposalTraceNotFound,
        ProposalTraceUnavailable,
    ) as error:
        return _error(error)


@proposal_trace_api.route(
    "/proposal-trace/<path:source_path>/audits/<external_id>", methods=["GET"]
)
def get_audit(source_path, external_id):
    try:
        if request.args:
            raise ProposalTraceInvalidRequest("unsupported query parameter")
        source_id = parse_source_path(source_path)
        authorized, policy = _authorize(source_id)
        if not authorized:
            return jsonify({"error": "unauthorized"}), 401
        body = _service().audit(source_id, external_id)
        return _response(body, policy)
    except (
        ProposalTraceInvalidRequest,
        ProposalTraceNotFound,
        ProposalTraceUnavailable,
    ) as error:
        return _error(error)

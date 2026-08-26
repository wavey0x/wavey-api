"""Minimal WSGI application for co-located Proposal Trace API deployment."""

import os

from flask import Flask
from flask_cors import CORS

from proposal_trace_routes import proposal_trace_api


def _integer(name, default):
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError("{} must be an integer".format(name)) from exc


def create_app():
    database = os.getenv("PROPOSAL_TRACE_DB_PATH")
    if database is None or not database.strip():
        raise RuntimeError("PROPOSAL_TRACE_DB_PATH must be set")
    app = Flask(__name__)
    app.config.update(
        PROPOSAL_TRACE_DB_PATH=database.strip(),
        PROPOSAL_TRACE_BUSY_TIMEOUT_MS=_integer("PROPOSAL_TRACE_BUSY_TIMEOUT_MS", 5000),
        PROPOSAL_TRACE_API_KEY=os.getenv("PROPOSAL_TRACE_API_KEY"),
    )
    CORS(app, resources={r"/api/proposal-trace/*": {"origins": "*"}})
    app.register_blueprint(proposal_trace_api, url_prefix="/api")
    return app


app = create_app()

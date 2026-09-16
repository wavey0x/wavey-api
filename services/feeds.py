"""Read the last complete scheduled feed without reaching external services."""

import json

from flask import jsonify
from sqlalchemy import text

from database import db


def validate_schema():
    with db.engine.connect() as connection:
        version = connection.execute(text(
            "SELECT value FROM _migration_meta WHERE key='open_data_schema_version'"
        )).scalar_one_or_none()
        if version != '1':
            raise RuntimeError('Open Data schema must be explicitly imported before API startup')
        connection.execute(text('SELECT name,document_json FROM feed_snapshots LIMIT 0'))
        connection.execute(text('SELECT name,last_attempt_at,last_success_at,error FROM collector_state LIMIT 0'))


def get_feed(name):
    if name not in ('resupply', 'ybs'):
        raise ValueError('Unknown feed')
    # One statement gives the document and its status from the same SQLite view.
    row = db.session.execute(text('''SELECT f.document_json,f.collection_started_at,
        f.collection_finished_at,f.published_at,s.last_attempt_at,s.last_success_at,s.error
        FROM feed_snapshots AS f JOIN collector_state AS s ON s.name=f.name
        WHERE f.name=:name'''), {'name': name}).mappings().first()
    if row is None:
        response = jsonify({'error': 'feed_unavailable'})
        response.status_code = 503
        response.headers['Cache-Control'] = 'no-store'
        return response
    document = json.loads(row['document_json'])
    document['_meta'] = {key: row[key] for key in row if key != 'document_json'}
    response = jsonify(document)
    response.headers['Cache-Control'] = 'public, max-age=60'
    return response

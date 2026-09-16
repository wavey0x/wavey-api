import json
import sqlite3

from flask import Flask
import pytest
from sqlalchemy.engine import URL

from database import db, init_app
from services.feeds import get_feed, validate_schema


@pytest.fixture
def feed_app(tmp_path):
    path = tmp_path / 'shared.sqlite3'
    with sqlite3.connect(path) as c:
        c.executescript('''PRAGMA user_version=1;
            CREATE TABLE _migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE feed_snapshots(name TEXT PRIMARY KEY,document_json TEXT,
                collection_started_at INTEGER,collection_finished_at INTEGER,published_at INTEGER);
            CREATE TABLE collector_state(name TEXT PRIMARY KEY,last_attempt_at INTEGER,
                last_success_at INTEGER,error TEXT);''')
        c.executemany('INSERT INTO _migration_meta VALUES (?,?)', [
            ('manifest', json.dumps(dict(schema_version=1, snapshot_sha256='a'*64,
                status='data_verified', application_ready=False, alerts_enabled=False))),
            ('open_data_schema_version', '1')])
        for feed in ('resupply', 'ybs'):
            c.execute('INSERT INTO feed_snapshots VALUES (?,?,NULL,?,?)', (feed, json.dumps(dict(
                data={'amount': 123456789012345678901, 'label': 'unchanged'}, last_update=100,
                last_update_block=200)), 100, 1000))
            c.execute('INSERT INTO collector_state VALUES (?,?,?,?)', (feed, 1001, 100, 'collection_failed'))
    app = Flask(__name__)
    app.testing = True
    app.config.update(SQLALCHEMY_DATABASE_URI=URL.create(
        'sqlite+pysqlite', database=path.as_uri(), query={'mode': 'ro', 'uri': 'true'}),
        YEARN_IMPORT_SHA256='a'*64)
    init_app(app, allow_rehearsal=True)
    app.add_url_rule('/api/resupply/data', 'resupply', lambda: get_feed('resupply'))
    app.add_url_rule('/api/ybs/data', 'ybs', lambda: get_feed('ybs'))
    yield app, path
    with app.app_context():
        db.session.remove()
        db.engine.dispose()


def test_last_good_feed_survives_failed_collection_and_preserves_source_age(feed_app, monkeypatch):
    app, _ = feed_app
    import requests
    monkeypatch.setattr(requests.sessions.Session, 'request', lambda *a, **k: pytest.fail('Network access'))
    with app.app_context():
        validate_schema()
    for feed in ('resupply', 'ybs'):
        response = app.test_client().get('/api/' + feed + '/data')
        assert response.status_code == 200
        assert response.json['last_update'] == 100
        assert response.json['data']['amount'] == 123456789012345678901
        assert response.json['_meta']['published_at'] == 1000
        assert response.json['_meta']['last_success_at'] == 100
        assert response.json['_meta']['error'] == 'collection_failed'
        assert response.headers['Cache-Control'] == 'public, max-age=60'


def test_missing_feed_returns_uncached_unavailable(feed_app):
    app, path = feed_app
    with sqlite3.connect(path) as c:
        c.execute("DELETE FROM feed_snapshots WHERE name='resupply'")
    response = app.test_client().get('/api/resupply/data')
    assert response.status_code == 503
    assert response.headers['Cache-Control'] == 'no-store'


def test_startup_rejects_unprepared_schema(feed_app):
    app, path = feed_app
    with sqlite3.connect(path) as c:
        c.execute("DELETE FROM _migration_meta WHERE key='open_data_schema_version'")
    with app.app_context(), pytest.raises(RuntimeError, match='explicitly imported'):
        validate_schema()

"""Atomic publication of the two scheduled data feeds; no network work here."""

import json
import logging
import time

from utils.sqlite_store import Store

FEEDS = ('resupply', 'ybs')
SCHEMA = (
    '''CREATE TABLE feed_snapshots (
        name TEXT PRIMARY KEY CHECK(name IN ('resupply','ybs')),
        document_json TEXT NOT NULL CHECK(json_valid(document_json)),
        collection_started_at INTEGER,
        collection_finished_at INTEGER NOT NULL,
        published_at INTEGER NOT NULL)''',
    '''CREATE TABLE collector_state (
        name TEXT PRIMARY KEY CHECK(name IN ('resupply','ybs')),
        revision INTEGER NOT NULL DEFAULT 0,
        config_json TEXT NOT NULL CHECK(json_valid(config_json)),
        state_json TEXT NOT NULL CHECK(json_valid(state_json)),
        last_attempt_at INTEGER,
        last_success_at INTEGER,
        error TEXT)''',
)


class CollectionError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def ensure_schema(connection):
    row = connection.execute(
        "SELECT value FROM _migration_meta WHERE key='open_data_schema_version'"
    ).fetchone()
    if row is None or row[0] != '1':
        raise CollectionError('schema_not_prepared')


def validate_document(name, document):
    if name not in FEEDS or not isinstance(document, dict):
        raise CollectionError('invalid_document')
    if not isinstance(document.get('data'), dict) or not document['data']:
        raise CollectionError('empty_document')
    for field in ('last_update', 'last_update_block'):
        if type(document.get(field)) is not int or document[field] <= 0:
            raise CollectionError('invalid_source_metadata')
    if name == 'resupply':
        required = {'market_data', 'retention_program', 'authorizations', 'loan_repayment', 'sreusd'}
        if not required <= document['data'].keys() or not document['data']['market_data']:
            raise CollectionError('incomplete_resupply_document')
    encode(document)  # Reject non-finite values before opening the write transaction.


def run_collection(name, config, build, *, store=None, clock=time.time):
    """Build(previous_document, state) returns a complete document and next state."""
    if name not in FEEDS:
        raise ValueError('Unknown feed')
    store = store or Store.from_env()
    started = int(clock())

    def begin(c):
        ensure_schema(c)
        row = c.execute('SELECT * FROM collector_state WHERE name=?', (name,)).fetchone()
        snapshot = c.execute('SELECT document_json FROM feed_snapshots WHERE name=?', (name,)).fetchone()
        if row is None or snapshot is None:
            raise CollectionError('feed_not_imported')
        revision = row['revision'] + 1
        c.execute('UPDATE collector_state SET revision=?,last_attempt_at=?,error=NULL WHERE name=?',
                  (revision, started, name))
        return revision, json.loads(row['config_json']), json.loads(row['state_json']), json.loads(snapshot[0])

    revision, accepted, state, previous = store.write(begin)
    try:
        if accepted != config:
            raise CollectionError('configuration_changed')
        document, state = build(previous, state)
        validate_document(name, document)
        document_json, state_json = encode(document), encode(state)
        finished = int(clock())

        def publish(c):
            ensure_schema(c)
            updated = c.execute('''UPDATE collector_state
                SET state_json=?,last_success_at=?,error=NULL
                WHERE name=? AND revision=?''', (state_json, finished, name, revision))
            if updated.rowcount != 1:
                raise CollectionError('concurrent_collection')
            c.execute('''UPDATE feed_snapshots SET document_json=?,collection_started_at=?,
                collection_finished_at=?,published_at=? WHERE name=?''',
                (document_json, started, finished, finished, name))
        store.write(publish)
        return document
    except Exception as error:
        code = error.code if isinstance(error, CollectionError) else 'collection_failed'
        # A superseded attempt must not overwrite the newer attempt's status.
        store.write(lambda c: c.execute(
            'UPDATE collector_state SET error=? WHERE name=? AND revision=?', (code, name, revision)))
        logging.getLogger(__name__).error('%s collection failed (%s)', name, code)
        raise

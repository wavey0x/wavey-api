"""One-time import of frozen Open Data files into an existing shared database."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import time

from utils.feed_chain import block_hash, verify_block
from utils.feed_config import feed_config, HISTORY_STARTS
from utils.feeds import SCHEMA, CollectionError, encode, ensure_schema, validate_document
from utils.sqlite_store import Store

INPUTS = (
    'feeds/resupply_market_data.json', 'feeds/ybs_data.json',
    'state/withdrawal_feed_cache.json', 'state/authorizations_cache.json',
    'state/loan_repayment_cache.json', 'state/ybs_data.json',
)


def read_inputs(directory):
    digest = hashlib.sha256()
    documents = {}
    for name in INPUTS:
        raw = (Path(directory) / name).read_bytes()
        digest.update(name.encode() + b'\0' + hashlib.sha256(raw).digest())
        documents[name] = json.loads(raw)
        encode(documents[name])
    return documents, digest.hexdigest()


def prepare(documents, w3):
    if w3.eth.chain_id != 1:
        raise CollectionError('wrong_chain')
    finalized = w3.eth.get_block('finalized')['number']
    snapshots = {'resupply': documents[INPUTS[0]], 'ybs': documents[INPUTS[1]]}
    for name, snapshot in snapshots.items():
        validate_document(name, snapshot)

    def cursor(height):
        if type(height) is not int or height < 0 or height > finalized:
            raise CollectionError('import_boundary_not_finalized')
        return {'block': height, 'hash': block_hash(w3.eth.get_block(height))}

    market = snapshots['resupply']['data']
    caches = {
        'withdrawals': documents['state/withdrawal_feed_cache.json'],
        'authorizations': documents['state/authorizations_cache.json'],
        'loans': documents['state/loan_repayment_cache.json'],
    }
    previous = {
        'withdrawals': {'feed': market['retention_program']['withdrawal_feed']},
        'authorizations': {'authorizations': market['authorizations']['all']},
        'loans': {k: market['loan_repayment'][k] for k in
                  ('repayments', 'bad_debt_payments', 'bad_debt_history', 'yearn_loan_history')},
    }
    state = {'resupply': {'cursors': {}, 'pending': {}}, 'ybs': {'cursors': {}, 'retained': {}}}
    for name, cache in caches.items():
        height = cache['last_processed_block']
        if height < HISTORY_STARTS[name]:
            raise CollectionError('import_cursor_before_deployment')
        state['resupply']['cursors'][name] = cursor(height)
        history = {k: cache[k] for k in previous[name]}
        for records in history.values():
            if not isinstance(records, list):
                raise CollectionError('invalid_import_history')
            for record in records:
                # Withdrawal records historically called their block number ts.
                if record.get('block', record.get('ts', 0)) > height:
                    raise CollectionError('import_history_ahead_of_cursor')
        # A failed old run may have saved caches before publishing its feed.
        # Keep that extra state until the first complete SQLite publication.
        if history != previous[name]:
            state['resupply']['pending'][name] = history

    weekly = documents['state/ybs_data.json']
    if not isinstance(weekly, dict):
        raise CollectionError('invalid_weekly_history')
    published = snapshots['ybs']['data']
    for token, history in weekly.items():
        visible = published.get(token, {}).get('ybs_data', {}).get('weekly_data')
        if history != {'weekly_data': visible}:
            state['ybs']['retained'][token] = history
    state['ybs']['cursors']['weekly'] = cursor(snapshots['ybs']['last_update_block'])
    for feed in state.values():
        for boundary in feed['cursors'].values():
            verify_block(w3, boundary['block'], boundary['hash'])
    return snapshots, state


def import_directory(directory, store, w3):
    documents, source_hash = read_inputs(directory)
    marker = store.read(lambda c: c.execute(
        "SELECT value FROM _migration_meta WHERE key='open_data_import_sha256'").fetchone())
    if marker:
        if marker[0] != source_hash:
            raise CollectionError('different_import_already_installed')
        store.read(ensure_schema)
        return {'status': 'already_imported', 'source_sha256': source_hash}
    snapshots, state = prepare(documents, w3)
    configs = {name: encode(feed_config(name)) for name in snapshots}
    now = int(time.time())

    def migrate(c):
        if c.execute("SELECT 1 FROM _migration_meta WHERE key='open_data_schema_version'").fetchone():
            raise CollectionError('schema_already_installed')
        for statement in SCHEMA:
            c.execute(statement)
        for name, document in snapshots.items():
            collected_at = document['last_update']
            c.execute('INSERT INTO feed_snapshots VALUES (?,?,NULL,?,?)',
                      (name, encode(document), collected_at, now))
            c.execute('''INSERT INTO collector_state
                (name,config_json,state_json,last_success_at) VALUES (?,?,?,?)''',
                (name, configs[name], encode(state[name]), collected_at))
        c.executemany('INSERT INTO _migration_meta VALUES (?,?)', (
            ('open_data_schema_version', '1'), ('open_data_import_sha256', source_hash)))
    store.write(migrate)
    return {'status': 'imported', 'source_sha256': source_hash, 'feeds': list(snapshots)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--rpc-url', default=os.getenv('OPEN_DATA_RPC_URL'))
    args = parser.parse_args()
    if not args.rpc_url:
        parser.error('--rpc-url or OPEN_DATA_RPC_URL is required')
    from web3 import Web3
    w3 = Web3(Web3.HTTPProvider(args.rpc_url, request_kwargs={'timeout': 30}))
    store = Store(args.database, os.environ['YEARN_IMPORT_SHA256'])
    print(json.dumps(import_directory(args.source, store, w3), indent=2))


if __name__ == '__main__':
    main()

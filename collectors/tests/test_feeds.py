from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
import copy
import json
from pathlib import Path
import sqlite3
from threading import Event
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from import_feeds import import_directory, INPUTS
from utils.feed_chain import advance, event_logs, finalized_boundary
from utils.feed_config import feed_config
from utils.feeds import CollectionError, run_collection
from utils.sqlite_store import Store


class Chain:
    chain_id = 1
    height = 30_000_000

    def __init__(self):
        self.eth = self
        self.changed = {}

    def get_block(self, height):
        if height == 'finalized':
            height = self.height
        return {'number': height, 'hash': self.changed.get(height, f'{height:064x}')}


class FeedTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.path = self.directory / 'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as c:
            c.executescript('''PRAGMA user_version=1;
                CREATE TABLE _migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE unrelated(id INTEGER PRIMARY KEY,value TEXT);
                INSERT INTO unrelated VALUES(1,'unchanged');''')
            c.execute('INSERT INTO _migration_meta VALUES (?,?)', ('manifest', json.dumps(dict(
                schema_version=1, snapshot_sha256='a'*64, status='data_verified',
                application_ready=False, alerts_enabled=False))))
            c.commit()
        self.store = Store(self.path, 'a'*64, rehearsal=True)
        self.chain = Chain()
        self.document = dict(data=dict(market_data=[{'exact': 123456789012345678901}],
            retention_program={'withdrawal_feed': []}, authorizations={'all': [], 'active': []},
            loan_repayment=dict(repayments=[], bad_debt_payments=[], bad_debt_history=[], yearn_loan_history=[]),
            sreusd={}), last_update=1700000000, last_update_block=29_000_000)
        self.weekly = {'active': {'weekly_data': {'1': {'value': 1.25}}},
                       'inactive': {'weekly_data': {'0': {'value': 3.75}}}}
        self.inputs = [self.document,
            dict(data={'active': {'ybs_data': self.weekly['active']}}, last_update=1700000000, last_update_block=29_000_000),
            dict(feed=[], last_processed_block=29_000_000),
            dict(authorizations=[], last_processed_block=29_000_000),
            dict(self.document['data']['loan_repayment'], last_processed_block=29_000_000), self.weekly]
        self.write_inputs()

    def write_inputs(self):
        for name, data in zip(INPUTS, self.inputs):
            path = self.directory / name
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(data))
        (self.directory / 'resupply_position_cache.json').write_text('deliberately not imported')

    def imported(self):
        return import_directory(self.directory, self.store, self.chain)

    def read(self, table, name='resupply'):
        return self.store.read(lambda c: dict(c.execute(f'SELECT * FROM {table} WHERE name=?', (name,)).fetchone()))

    def test_import_preserves_values_inactive_history_and_shared_state(self):
        self.imported()
        self.assertEqual(json.loads(self.read('feed_snapshots')['document_json']), self.document)
        ybs = json.loads(self.read('collector_state', 'ybs')['state_json'])
        self.assertEqual(ybs['retained'], {'inactive': self.weekly['inactive']})
        self.assertEqual(self.store.read(lambda c: c.execute('SELECT value FROM unrelated').fetchone()[0]), 'unchanged')
        self.assertEqual(self.store.read(lambda c: c.execute('PRAGMA user_version').fetchone()[0]), 1)
        tables = self.store.read(lambda c: [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")])
        self.assertFalse(any('position' in name for name in tables))

    def test_identical_import_is_noop_after_new_data_and_needs_no_rpc(self):
        self.imported()
        self.run_success(42)
        before = self.read('feed_snapshots')
        self.assertEqual(import_directory(self.directory, self.store, None)['status'], 'already_imported')
        self.assertEqual(self.read('feed_snapshots'), before)
        self.inputs[0]['last_update'] += 1
        self.write_inputs()
        with self.assertRaisesRegex(CollectionError, 'different_import'):
            self.imported()

    def test_unpublished_cache_is_preserved_until_publication(self):
        extra = {'ts': 28_000_000, 'amount': 123.456}
        self.inputs[2]['feed'] = [extra]
        self.write_inputs()
        self.imported()
        state = json.loads(self.read('collector_state')['state_json'])
        self.assertEqual(state['pending']['withdrawals'], {'feed': [extra]})
        self.assertEqual(json.loads(self.read('feed_snapshots')['document_json']), self.document)

    def test_unfinalized_source_refuses_before_any_new_table(self):
        self.chain.height = 28_000_000
        with self.assertRaisesRegex(CollectionError, 'not_finalized'):
            self.imported()
        self.assertIsNone(self.store.read(lambda c: c.execute("SELECT name FROM sqlite_master WHERE name='feed_snapshots'").fetchone()))

    def test_import_failure_rolls_back_schema_and_all_new_rows(self):
        self.store.write(lambda c: c.execute('''CREATE TRIGGER fail_import BEFORE INSERT ON _migration_meta
            WHEN NEW.key='open_data_import_sha256' BEGIN SELECT RAISE(ABORT,'interrupted'); END'''))
        with self.assertRaises(sqlite3.IntegrityError):
            self.imported()
        self.assertIsNone(self.store.read(lambda c: c.execute("SELECT name FROM sqlite_master WHERE name='feed_snapshots'").fetchone()))

    def run_success(self, value, build_extra=None):
        def build(document, state):
            end, digest = finalized_boundary(self.chain, state)
            document['data']['market_data'][0]['exact'] = value
            if build_extra:
                build_extra()
            return document, advance(state, end, digest, self.chain)
        return run_collection('resupply', feed_config('resupply'), build, store=self.store)

    def test_empty_event_range_advances_output_and_cursor_together(self):
        self.imported()
        self.run_success(42)
        state = json.loads(self.read('collector_state')['state_json'])
        self.assertEqual({v['block'] for v in state['cursors'].values()}, {30_000_000})
        self.assertEqual(json.loads(self.read('feed_snapshots')['document_json'])['data']['market_data'][0]['exact'], 42)

    def test_rpc_and_publication_failures_leave_previous_output_and_progress(self):
        self.imported()
        before = self.read('feed_snapshots')
        state = self.read('collector_state')['state_json']
        def failed(document, next_state):
            next_state['cursors'].clear()
            raise OSError('private RPC URL must not appear in public status')
        with self.assertRaises(OSError):
            run_collection('resupply', feed_config('resupply'), failed, store=self.store)
        self.assertEqual(self.read('collector_state')['error'], 'collection_failed')
        self.store.write(lambda c: c.execute('''CREATE TRIGGER fail_publish BEFORE UPDATE ON feed_snapshots
            BEGIN SELECT RAISE(ABORT,'disk write failed'); END'''))
        with self.assertRaises(sqlite3.IntegrityError):
            self.run_success(99)
        self.assertEqual(self.read('feed_snapshots'), before)
        self.assertEqual(self.read('collector_state')['state_json'], state)

    def test_configuration_and_hash_changes_do_not_reset_history(self):
        self.imported()
        before = self.read('collector_state')['state_json']
        with self.assertRaisesRegex(CollectionError, 'configuration_changed'):
            run_collection('resupply', {}, lambda *args: self.fail('build called'), store=self.store)
        self.chain.changed[29_000_000] = 'b'*64
        with self.assertRaisesRegex(CollectionError, 'checkpoint_changed'):
            self.run_success(1)
        self.assertEqual(self.read('collector_state')['state_json'], before)

    def test_overlapping_attempt_cannot_overwrite_newer_result_or_status(self):
        self.imported()
        with self.assertRaisesRegex(CollectionError, 'concurrent_collection'):
            self.run_success(1, lambda: self.run_success(2))
        result = json.loads(self.read('feed_snapshots')['document_json'])
        self.assertEqual(result['data']['market_data'][0]['exact'], 2)
        self.assertIsNone(self.read('collector_state')['error'])

    def test_duplicate_logs_are_suppressed_without_losing_distinct_events(self):
        logs = [{'blockNumber': 12, 'logIndex': i, 'transactionHash': 'tx'} for i in (1, 1, 2)]
        source = SimpleNamespace(get_logs=lambda **kwargs: logs)
        self.assertEqual([r['logIndex'] for r in event_logs(source, 10, 15)], [1, 2])

    def test_network_phase_allows_shared_writes_reads_and_online_backup(self):
        self.imported()
        self.store.read(lambda c: c.execute('PRAGMA journal_mode=WAL'))
        started, release = Event(), Event()
        before = self.read('feed_snapshots')
        def network_work():
            started.set()
            if not release.wait(5):
                raise RuntimeError('Test collection was not released')
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.run_success, 42, network_work)
            try:
                self.assertTrue(started.wait(5))
                self.store.write(lambda c: c.execute("INSERT INTO unrelated VALUES(2,'concurrent writer')"))
                self.assertEqual(self.read('feed_snapshots'), before)
                backup = self.directory / 'online.sqlite3'
                with closing(sqlite3.connect(self.path)) as source, closing(sqlite3.connect(backup)) as target:
                    source.backup(target)
                    self.assertEqual(target.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                    self.assertEqual(target.execute('SELECT count(*) FROM unrelated').fetchone()[0], 2)
                    saved = target.execute("SELECT document_json FROM feed_snapshots WHERE name='resupply'").fetchone()[0]
                    self.assertEqual(saved, before['document_json'])
            finally:
                release.set()
            future.result(timeout=5)
        self.assertEqual(json.loads(self.read('feed_snapshots')['document_json'])['data']['market_data'][0]['exact'], 42)


if __name__ == '__main__':
    unittest.main()

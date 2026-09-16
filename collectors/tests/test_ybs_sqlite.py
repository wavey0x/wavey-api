from contextlib import closing
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from hexbytes import HexBytes

from utils import db
from utils.sqlite_store import Store
from scripts.ybs_dash.listeners import event_listener as listener
from scripts.ybs_dash.processes import user_data

IMPORT_ID = 'a' * 64
YBS, TOKEN, REWARDS = '0x1111', '0x2222', '0x3333'


def block_hash(n):
    return HexBytes(n.to_bytes(32, 'big'))


def event(kind, number=10, index=0, amount=10**18, week=1):
    stake = kind in ('Staked', 'Unstaked')
    args = dict(week=week, account='alice', depositor='alice', amount=amount, rewardAmount=amount,
                newUserWeight=amount, weightAdded=amount if kind == 'Staked' else 0,
                weightRemoved=amount if kind == 'Unstaked' else 0)
    return dict(event=kind, blockNumber=number, blockHash=block_hash(number), transactionHash=block_hash(1000+number),
                logIndex=index, address=YBS if stake else REWARDS, args=args)


class Chain:
    def __init__(self):
        self.eth = self
        self.chain_id = 1
        self.logs = []
        self.height = 12
        self.fail = False
        self.hash_changes = {}
        self.info = dict(ybs=SimpleNamespace(address=YBS, MAX_STAKE_GROWTH_WEEKS=lambda **kwargs: 4),
                         rewards=SimpleNamespace(address=REWARDS), decimals=18,
                         symbol='TOKEN', ybs_deploy_block=1, token=SimpleNamespace(address=TOKEN))
        for obj in (self.info['ybs'], self.info['rewards']):
            obj.events = SimpleNamespace(**{kind: SimpleNamespace(get_logs=self.getter(kind)) for kind in listener.EVENT_TYPES})

    def getter(self, kind):
        def get_logs(fromBlock, toBlock):
            if self.fail:
                raise OSError('RPC failed')
            return [v for v in self.logs if v['event'] == kind and fromBlock <= v['blockNumber'] <= toBlock]
        return get_logs

    def get_block(self, number):
        if number == 'finalized':
            number = self.height
        return dict(number=number, hash=self.hash_changes.get(number, block_hash(number)), timestamp=1700000000+number)

    def get_transaction_receipt(self, tx):
        logs = [v for v in self.logs if v['transactionHash'] == tx]
        return dict(transactionHash=tx, blockHash=logs[0]['blockHash'], logs=sorted(logs, key=lambda v:v['logIndex']))


class YBSTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / 'shared.sqlite3'
        with closing(sqlite3.connect(self.path)) as c:
            c.executescript('''
                PRAGMA user_version=1;
                CREATE TABLE _migration_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE stakes(id INTEGER PRIMARY KEY AUTOINCREMENT,ybs TEXT,is_stake INTEGER,account TEXT,
                    amount TEXT NOT NULL,new_weight TEXT,net_weight_change TEXT,week INTEGER,unlock_week INTEGER,
                    txn_hash TEXT,block INTEGER,timestamp INTEGER,date_str TEXT,token TEXT);
                CREATE TABLE rewards(id INTEGER PRIMARY KEY AUTOINCREMENT,ybs TEXT,reward_distributor TEXT,
                    is_claim INTEGER,account TEXT,amount TEXT,week INTEGER,txn_hash TEXT,block INTEGER,
                    timestamp INTEGER,date_str TEXT,token TEXT);
                CREATE TABLE stake_buckets(token TEXT,unlock_week INTEGER,net_amount TEXT,PRIMARY KEY(token,unlock_week));
                CREATE TABLE week_info(week_id INTEGER,token TEXT,weight TEXT,total_supply TEXT,boost TEXT,ybs TEXT,
                    stake_map TEXT,start_ts INTEGER,end_ts INTEGER,start_block INTEGER,end_block INTEGER,
                    start_time_str TEXT,end_time_str TEXT,UNIQUE(week_id,ybs));
                CREATE TABLE user_info(account TEXT,week_id INTEGER,token TEXT,weight TEXT,balance TEXT,boost TEXT,
                    stake_map TEXT,rewards_earned TEXT,total_realized TEXT,ybs TEXT,UNIQUE(account,week_id,ybs));
            ''')
            c.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(dict(
                schema_version=1,snapshot_sha256=IMPORT_ID,status='data_verified',application_ready=False,alerts_enabled=False))))
            c.commit()
        self.store = Store(self.path, IMPORT_ID, rehearsal=True)
        db.prepare(self.store)
        p = patch.object(db, 'get_store', return_value=self.store)
        p.start()
        self.addCleanup(p.stop)
        self.chain = Chain()

    def seed(self, start=10):
        self.store.write(lambda c:c.execute('INSERT INTO ybs_checkpoints VALUES (?,?,?,?,?,?)',
            (YBS,TOKEN,1,start,start,listener.hex_value(block_hash(start-1)))))

    def scan(self):
        listener.process_token_events(TOKEN,self.chain.info,self.chain.height,store=self.store,w3=self.chain)

    def state(self):
        return self.store.read(lambda c:db.checkpoint(c,YBS))

    def count(self, table):
        return self.store.read(lambda c:c.execute(f'SELECT count(*) FROM {table}').fetchone()[0])

    def bucket(self, week):
        return self.store.read(lambda c:db.get_stake_bucket_amount(c,TOKEN,week))

    def records(self):
        return listener.collect(self.chain,TOKEN,self.chain.info,{k:(10,12) for k in listener.EVENT_TYPES})[0]

    def test_events_buckets_and_cursor_follow_chain_order_and_survive_restart(self):
        self.seed()
        self.chain.logs = [event('Staked',10,amount=2*10**18),event('Unstaked',11),event('Staked',12,amount=3*10**18,week=2)]
        self.scan()
        self.assertEqual(self.bucket(5),Decimal(1))
        self.assertEqual(self.bucket(6),Decimal(3))
        self.assertEqual(self.count('stakes'),3)
        self.assertEqual(self.state()['next_block'],13)
        self.chain.height = 14
        self.scan()
        self.assertEqual(self.state()['next_block'],15)
        self.assertEqual(self.count('stakes'),3)

    def test_duplicate_event_never_updates_bucket_twice(self):
        self.seed()
        self.chain.logs = [event('Staked',amount=10**18+1)]
        items = self.records()
        db.commit_events(self.store,self.state(),items+items,12,listener.hex_value(block_hash(12)),4)
        self.assertEqual(self.count('stakes'),1)
        self.assertEqual(self.count('ybs_events'),1)
        self.assertEqual(self.bucket(5),Decimal('1.000000000000000001'))

    def test_failed_bucket_write_rolls_back_event_and_cursor(self):
        self.seed()
        self.chain.logs = [event('Staked')]
        with patch.object(db,'apply_stake',side_effect=RuntimeError('interrupted')), self.assertRaisesRegex(RuntimeError,'interrupted'):
            self.scan()
        self.assertEqual(self.count('stakes'),0)
        self.assertEqual(self.count('ybs_events'),0)
        self.assertEqual(self.state()['next_block'],10)
        self.scan()
        self.assertEqual(self.bucket(5),Decimal(1))

    def test_failed_rpc_and_reorg_do_not_advance(self):
        self.seed()
        self.chain.fail = True
        with self.assertRaises(OSError):
            self.scan()
        self.chain.fail = False
        self.chain.hash_changes[9] = block_hash(999)
        with self.assertRaisesRegex(RuntimeError,'block changed'):
            self.scan()
        self.assertEqual(self.state()['next_block'],10)

    def test_missing_cursor_and_concurrent_advance_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError,'checkpoint missing'):
            self.scan()
        self.seed()
        old = self.state()
        db.commit_events(self.store,old,[],10,listener.hex_value(block_hash(10)),4)
        with self.assertRaisesRegex(RuntimeError,'concurrently'):
            db.commit_events(self.store,old,[],12,listener.hex_value(block_hash(12)),4)
        self.assertEqual(self.state()['next_block'],11)

    def import_boundary(self):
        self.chain.logs = [event(k,index=i,amount=1234567890123456789) for i,k in enumerate(listener.EVENT_TYPES)]
        items = listener.collect(self.chain,TOKEN,self.chain.info,{k:(10,12) for k in listener.EVENT_TYPES},legacy_values=True)[0]
        def write(c):
            for item in items:
                record = dict(item['record'], **item['legacy'])
                db.insert(c,'stakes' if item['event_type'] in ('Staked','Unstaked') else 'rewards',record)
            db.upsert_stake_bucket(c,TOKEN,5,Decimal('9.123456789012345678'))
        self.store.write(write)
        return {YBS:{kind:13 for kind in listener.EVENT_TYPES}}

    def test_adoption_validates_history_and_preserves_buckets(self):
        cursor = self.import_boundary()
        result = listener.adopt_cursor(self.store,self.chain,TOKEN,self.chain.info,cursor)
        self.assertEqual(result['validated_boundary_events'],4)
        self.assertEqual(self.count('stakes'),2)
        self.assertEqual(self.count('rewards'),2)
        self.assertEqual(self.bucket(5),Decimal('9.123456789012345678'))
        self.assertEqual(self.state()['next_block'],13)

    def test_adoption_rejects_unrecorded_events_and_disagreeing_cursors(self):
        cursor = self.import_boundary()
        self.chain.logs.append(event('Staked',11))
        with self.assertRaisesRegex(RuntimeError,'boundary differs'):
            listener.adopt_cursor(self.store,self.chain,TOKEN,self.chain.info,cursor)
        self.assertEqual(self.count('ybs_checkpoints'),0)
        cursor[YBS]['Staked'] = 12
        with self.assertRaisesRegex(RuntimeError,'disagree'):
            listener.adopt_cursor(self.store,self.chain,TOKEN,self.chain.info,cursor)

    def week_record(self, end=12):
        return dict(week_id=1,ybs=YBS,token=TOKEN,end_block=end,weight=Decimal('2.000000000000000001'),
                    total_supply=Decimal(1),boost=Decimal(2),stake_map={'realized':Decimal('1.123456789012345678')})

    def user_record(self):
        return dict(account='alice',week_id=1,ybs=YBS,token=TOKEN,weight=Decimal(1),balance=Decimal(1),
                    boost=Decimal(1),stake_map={'large':Decimal('12345678901234567890.123456789012345678')},
                    rewards_earned=Decimal('0.000000000000000001'),total_realized=Decimal(1))

    def test_week_and_users_publish_atomically_with_exact_json_numbers(self):
        db.publish_week(self.week_record(),[self.user_record()],None)
        self.assertTrue(db.snapshot_has_baseline(YBS,1))
        row = self.store.read(lambda c:dict(c.execute('SELECT * FROM user_info').fetchone()))
        self.assertEqual(row['rewards_earned'],'0.000000000000000001')
        self.assertEqual(json.loads(row['stake_map'],parse_float=Decimal)['large'],Decimal('12345678901234567890.123456789012345678'))
        self.assertEqual(db.week_snapshot(YBS,1)['end_block'],12)
        with self.assertRaisesRegex(RuntimeError,'concurrently'):
            db.publish_week(self.week_record(13),[self.user_record()],11)
        self.assertEqual(db.week_snapshot(YBS,1)['end_block'],12)

    def test_invalid_user_rolls_back_entire_week_and_baseline(self):
        wrong = dict(self.user_record(),week_id=2)
        with self.assertRaisesRegex(ValueError,'different week'):
            db.publish_week(self.week_record(),[self.user_record(),wrong],None)
        self.assertEqual(self.count('week_info'),0)
        self.assertEqual(self.count('user_info'),0)
        self.assertFalse(db.snapshot_has_baseline(YBS,1))

    def test_failed_snapshot_calculation_does_not_publish_partial_week(self):
        with patch.object(db,'query_unique_accounts',return_value=['alice']), patch.object(user_data,'build_week_record',return_value=self.week_record()), patch.object(user_data,'build_user_records',side_effect=OSError('RPC failed')):
            self.chain.info['ybs'].decimals = lambda **kwargs:18
            self.chain.info['ybs'].MAX_STAKE_GROWTH_WEEKS = lambda **kwargs:4
            with self.assertRaises(OSError):
                user_data.refresh_week(TOKEN,self.chain.info,1,12,self.chain,SimpleNamespace())
        self.assertEqual(self.count('week_info'),0)
        self.assertEqual(self.count('ybs_snapshot_progress'),0)

    def test_snapshot_removes_zero_weight_user_in_same_transaction(self):
        db.publish_week(self.week_record(),[self.user_record()],None)
        db.publish_week(self.week_record(13),[],12,['alice'])
        self.assertEqual(self.count('user_info'),0)
        self.assertEqual(db.week_snapshot(YBS,1)['end_block'],13)

    def test_float_amounts_and_float_json_are_rejected(self):
        with self.assertRaises(TypeError):
            db.publish_week(dict(self.week_record(),weight=1.2),[],None)
        with self.assertRaises(TypeError):
            db.json_numbers({'amount':1.2})
        self.assertEqual(self.count('week_info'),0)

    def test_reward_deposit_refreshes_every_account(self):
        self.seed()
        self.chain.logs = [event('RewardsClaimed')]
        self.scan()
        self.assertEqual(db.changed_accounts(TOKEN,10,12),['alice'])
        self.chain.logs.append(event('RewardDeposited',13))
        self.chain.height = 13
        self.scan()
        self.assertIsNone(db.changed_accounts(TOKEN,13,13))
        self.assertEqual(db.changed_accounts(TOKEN,14,14),[])

    def test_snapshot_contract_calls_share_one_block_and_handle_zero_weight(self):
        calls=[]
        def method(result):
            def invoke(*args,**kwargs):
                calls.append(kwargs.get('block_identifier'))
                return result(*args) if callable(result) else result
            return invoke
        ybs=SimpleNamespace(address=YBS,totalSupply=method(2*10**18),getGlobalWeightAt=method(4*10**18),
            globalWeeklyToRealize=method({'weight':0}),getAccountWeightAt=method(lambda user,week:10**18 if user=='alice' else 0),
            balanceOf=method(10**18),accountData=method({'lastUpdateWeek':1,'updateWeeksBitmap':0,'realizedStake':5*10**17}),
            accountWeeklyToRealize=method({'weight':0}))
        info=dict(ybs=ybs,token=SimpleNamespace(address=TOKEN),rewards=SimpleNamespace(getClaimableAt=method(1)),
                  reward_token=SimpleNamespace(decimals=method(18)))
        utilities=SimpleNamespace(get_week_start_ts=lambda *args:1700000000,get_week_end_ts=lambda *args:1700604799,
                                  get_week_start_block=lambda *args:1)
        week=user_data.build_week_record(info,1,12,4,18,utilities)
        users,removed=user_data.build_user_records(['alice','zero'],info,1,12,4,18,utilities)
        self.assertEqual(set(calls),{12})
        self.assertEqual(week['stake_map']['realized'],Decimal(2))
        self.assertEqual(users[0]['rewards_earned'],Decimal('0.000000000000000001'))
        self.assertEqual(removed,['zero'])

    def test_inactive_import_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'not ready'):
            Store(self.path,IMPORT_ID)


if __name__ == '__main__':
    unittest.main()

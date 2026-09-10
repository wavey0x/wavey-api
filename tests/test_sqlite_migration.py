import json
from pathlib import Path
import sqlite3

from flask import Flask, request
import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

from database import db, init_app
from services import crvlol, gauge_voting, resupply, stakes, ybs


IMPORT_ID = 'a' * 64
GAUGE = '0xbe0451815b546F705ef3f398B8179aE3AADDA14e'


@pytest.fixture
def database_path(tmp_path):
    path = tmp_path / 'shared data.sqlite3'
    with sqlite3.connect(path) as connection:
        connection.executescript('''
            PRAGMA user_version=1;
            CREATE TABLE _migration_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE stakes (id INTEGER PRIMARY KEY,ybs TEXT,account TEXT,amount TEXT,
                new_weight TEXT,timestamp INTEGER,is_stake INTEGER);
            CREATE TABLE crv_ll_harvests (id INTEGER PRIMARY KEY,profit TEXT,timestamp INTEGER,
                name TEXT,underlying TEXT,compounder TEXT,block INTEGER,txn_hash TEXT,date_str TEXT);
            CREATE TABLE curve_gauge_votes (id INTEGER PRIMARY KEY,gauge TEXT,gauge_name TEXT,
                account TEXT,amount TEXT,weight INTEGER,txn_hash TEXT,timestamp INTEGER,
                date_str TEXT,block INTEGER,account_alias TEXT);
            CREATE TABLE week_info (week_id INTEGER,token TEXT,weight TEXT,total_supply TEXT,
                boost TEXT,stake_map TEXT,ybs TEXT,start_ts INTEGER,end_ts INTEGER,
                start_block INTEGER,end_block INTEGER,start_time_str TEXT,end_time_str TEXT);
            CREATE TABLE user_info (account TEXT,week_id INTEGER,token TEXT,weight TEXT,balance TEXT,
                boost TEXT,stake_map TEXT,rewards_earned TEXT,ybs TEXT);
            CREATE VIEW user_week_info AS SELECT user_info.account,user_info.week_id,
                user_info.weight AS user_weight,user_info.balance AS user_balance,
                user_info.boost AS user_boost,user_info.stake_map AS user_stake_map,
                user_info.rewards_earned AS user_rewards_earned,user_info.ybs,week_info.token,
                week_info.weight AS global_weight,week_info.stake_map AS global_stake_map,
                week_info.start_ts,week_info.start_block,week_info.end_ts,week_info.end_block,
                week_info.start_time_str,week_info.end_time_str
                FROM user_info JOIN week_info ON user_info.week_id=week_info.week_id
                AND user_info.token=week_info.token;
            CREATE TABLE incentives (id INTEGER PRIMARY KEY,protocol TEXT,epoch INTEGER,
                total_incentives REAL,votium_amount REAL,votemarket_amount REAL,
                votium_votes_per_usd REAL,votemarket_votes_per_usd REAL,votium_votes REAL,
                votemarket_votes REAL,gauge_data TEXT,transaction_hash TEXT,block_number INTEGER,
                timestamp INTEGER,date_str TEXT,period_start INTEGER,log_index INTEGER);
        ''')
        manifest = {'schema_version': 1, 'status': 'data_verified', 'application_ready': False,
                    'alerts_enabled': False, 'snapshot_sha256': IMPORT_ID}
        connection.execute('INSERT INTO _migration_meta VALUES (?,?)',('manifest',json.dumps(manifest)))
        connection.executemany('INSERT INTO stakes VALUES (?,?,?,?,?,?,?)',[
            (1,'staking','account','1.123456789012345678','9.000000000000000001',20,1),
            (2,'staking','account','2','10',100,0)])
        connection.execute('INSERT INTO crv_ll_harvests VALUES (?,?,?,?,?,?,?,?,?)',
            (1,'123456789012.123456789012345678',100,'harvest','asset','compounder',10,'tx','date'))
        connection.execute('INSERT INTO curve_gauge_votes VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (1,GAUGE,'Gauge','account','1000000.123456789012345678',100,'tx',100,'date',10,'Alias'))
        connection.execute('INSERT INTO week_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (1,'TOKEN','100','200','0.5','{"bucket":100}','staking',10,20,1,2,'start','end'))
        connection.execute('INSERT INTO user_info VALUES (?,?,?,?,?,?,?,?,?)',
            ('account',1,'TOKEN','10','20','0.5','{"bucket":10}','0.123456789012345678','staking'))
        connection.executemany('INSERT INTO incentives VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',[
            (1,'resupply',1,100.5,50,50.5,None,1.5,200,250,'{"large":12345678901234567890}','tx',10,100,'date',50,0),
            (2,'yieldbasis',1,20.5,10,10.5,2.1,None,100,150,'{}','tx2',11,101,'date',50,1)])
    return path


def configured_app(path, import_id=IMPORT_ID):
    app = Flask(__name__)
    app.testing = True
    app.config.update(SQLALCHEMY_DATABASE_URI=URL.create(
        'sqlite+pysqlite',database=path.as_uri(),query={'mode':'ro','uri':'true'}),
        YEARN_IMPORT_SHA256=import_id)
    return app


@pytest.fixture
def application(database_path):
    app = configured_app(database_path)
    init_app(app,allow_rehearsal=True)
    yield app
    with app.app_context():
        db.session.remove()
        db.engine.dispose()


def test_stakes_preserve_numbers_and_numeric_timestamp_order(application):
    with application.test_request_context('/?page=1&per_page=1'):
        response,status = stakes.get_stakes_paged(request)
        assert status == 200
        assert response.get_json() == [{'id':2,'ybs':'staking','staked':False,
            'account':'account','amount':2.0,'newweight':10.0,'timestamp':100}]


def test_harvest_preserves_exact_existing_string_response(application):
    with application.test_request_context('/'):
        result = crvlol.get_harvests().get_json()
        assert result['total'] == 1
        assert result['data'][0]['profit'] == '123456789012.123456789012345678'


def test_ybs_view_and_global_response(application):
    with application.test_request_context('/?account=account&week_id=1&token=TOKEN'):
        user = ybs.user_info(request).get_json()[0]
        assert user['weight'] == 10.0
        assert user['global_weight'] == 100.0
        assert user['stake_map'] == {'bucket':10}
        assert user['global_stake_map'] == {'bucket':100}
        assert user['rewards_earned'] == float('0.123456789012345678')
    with application.test_request_context('/?week_id=1&token=TOKEN'):
        global_info = ybs.global_info(request).get_json()[0]
        assert global_info['total_supply'] == global_info['balance'] == 200.0
    with application.test_request_context('/?week_id=99&token=TOKEN'):
        assert ybs.global_info(request).get_json() == []


def test_gauge_votes_filter_pagination_and_number_contract(application):
    with application.test_request_context('/?gauge='+GAUGE+'&page=1&per_page=1'):
        result = gauge_voting.get_gauge_votes(request).get_json()
        assert result['total'] == 1
        assert result['data'][0]['amount'] == float('1000000.123456789012345678')
    with application.test_request_context('/?gauge=invalid'):
        assert gauge_voting.get_gauge_votes(request)[1] == 400


def test_incentives_preserve_protocol_filter_null_and_json(application):
    with application.test_request_context('/?protocol=resupply&epoch=1&limit=1'):
        response,status = resupply.incentive_report(request)
        result = response.get_json()
        assert status == 200
        assert result['pagination']['total'] == 1
        assert result['data'][0]['votium_votes_per_usd'] is None
        assert result['data'][0]['gauge_data']['large'] == 12345678901234567890
        assert result['data'][0]['total_incentives'] == 100.5


def test_database_rejects_writes(application):
    with application.app_context(),db.engine.connect() as connection:
        assert connection.execute(text('PRAGMA query_only')).scalar_one() == 1
        assert connection.execute(text('PRAGMA foreign_keys')).scalar_one() == 1
        with pytest.raises(OperationalError,match='readonly'):
            connection.execute(text('DELETE FROM stakes'))


def test_missing_database_is_never_created(tmp_path):
    path = tmp_path/'missing.sqlite3'
    with pytest.raises(OperationalError):
        init_app(configured_app(path),allow_rehearsal=True)
    assert not path.exists()


def test_wrong_import_is_rejected(database_path):
    with pytest.raises(RuntimeError,match='identity'):
        init_app(configured_app(database_path,'b'*64),allow_rehearsal=True)


def test_rehearsal_import_cannot_start_production(database_path):
    with pytest.raises(RuntimeError,match='not ready'):
        init_app(configured_app(database_path))
    app = configured_app(database_path)
    app.testing = False
    with pytest.raises(RuntimeError,match='only be opened by test'):
        init_app(app,allow_rehearsal=True)


def test_configuration_requires_explicit_sqlite_import(monkeypatch,tmp_path):
    from config import Config
    monkeypatch.setenv('DATABASE_URI','postgresql://unused.invalid/database')
    monkeypatch.delenv('YEARN_DB_PATH',raising=False)
    with pytest.raises(RuntimeError,match='YEARN_DB_PATH'):
        Config()
    path=tmp_path/'shared state.sqlite3'
    monkeypatch.setenv('YEARN_DB_PATH',str(path))
    monkeypatch.setenv('YEARN_IMPORT_SHA256',IMPORT_ID)
    monkeypatch.setenv('TIDAL_DB_PATH',str(tmp_path/'tidal.sqlite3'))
    config=Config()
    assert config.SQLALCHEMY_DATABASE_URI.drivername == 'sqlite+pysqlite'
    assert config.SQLALCHEMY_DATABASE_URI.query['mode'] == 'ro'
    assert config.SQLALCHEMY_DATABASE_URI.database == path.as_uri()


def test_ready_import_still_requires_production_wal(database_path):
    with sqlite3.connect(database_path) as connection:
        manifest=json.loads(connection.execute("SELECT value FROM _migration_meta").fetchone()[0])
        manifest.update(status='ready',application_ready=True)
        connection.execute('UPDATE _migration_meta SET value=?',(json.dumps(manifest),))
    with pytest.raises(RuntimeError,match='must use WAL'):
        init_app(configured_app(database_path))


def test_wal_import_checks_the_library_actually_loaded(database_path):
    with sqlite3.connect(database_path) as connection:
        manifest=json.loads(connection.execute("SELECT value FROM _migration_meta").fetchone()[0])
        manifest.update(status='ready',application_ready=True)
        connection.execute('UPDATE _migration_meta SET value=?',(json.dumps(manifest),))
        connection.commit()
        connection.execute('PRAGMA journal_mode=WAL')
    connection.close()  # The API must start with no existing writer connection.
    runtime=sqlite3.sqlite_version_info
    patched=(runtime >= (3,51,3) or (3,50,7) <= runtime < (3,51,0)
             or (3,44,6) <= runtime < (3,45,0))
    app=configured_app(database_path)
    if patched:
        init_app(app)
        with app.app_context():
            db.engine.dispose()
    else:
        with pytest.raises(RuntimeError,match='WAL-reset fix'):
            init_app(app)

"""YBS storage. Network calls belong outside these atomic SQLite operations."""

from decimal import Decimal, localcontext
from functools import lru_cache
import json

from utils.sqlite_store import Store

DEPLOY_BLOCK = 19888353
NUMERIC = {
    'stakes': {'amount', 'new_weight', 'net_weight_change'},
    'rewards': {'amount'},
    'week_info': {'weight', 'total_supply', 'boost'},
    'user_info': {'weight', 'balance', 'boost', 'rewards_earned', 'total_realized'},
}
COLUMNS = {
    'stakes': {'ybs', 'is_stake', 'account', 'amount', 'new_weight', 'net_weight_change', 'week',
               'unlock_week', 'txn_hash', 'block', 'timestamp', 'date_str', 'token'},
    'rewards': {'ybs', 'reward_distributor', 'is_claim', 'account', 'amount', 'week', 'txn_hash',
                'block', 'timestamp', 'date_str', 'token'},
    'week_info': {'week_id', 'token', 'weight', 'total_supply', 'boost', 'ybs', 'stake_map',
                 'start_ts', 'end_ts', 'start_block', 'end_block', 'start_time_str', 'end_time_str'},
    'user_info': {'account', 'week_id', 'token', 'weight', 'balance', 'boost', 'stake_map',
                 'rewards_earned', 'total_realized', 'ybs'},
}


@lru_cache(maxsize=1)
def get_store():
    return Store.from_env()


def exact(value):
    if isinstance(value, float):
        raise TypeError('Exact amounts must not pass through float')
    value = Decimal(value)
    if not value.is_finite():
        raise ValueError('Exact amounts must be finite')
    return format(value, 'f')


def scaled(raw, decimals):
    with localcontext() as context:
        context.prec = 100
        return Decimal(raw).scaleb(-decimals)


def json_numbers(value):
    """Retain JSON number types while serializing Decimal without float rounding."""
    if isinstance(value, Decimal):
        return exact(value)
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(str(k)) + ':' + json_numbers(v) for k, v in value.items()) + '}'
    if isinstance(value, (list, tuple)):
        return '[' + ','.join(json_numbers(v) for v in value) + ']'
    if isinstance(value, float):
        raise TypeError('Calculated JSON amounts must not pass through float')
    return json.dumps(value, allow_nan=False)


def insert(connection, table, record, *, upsert=False):
    if table not in COLUMNS or not record or not set(record) <= COLUMNS[table]:
        raise ValueError('Unexpected YBS record columns')
    columns = list(record)
    values = [None if record[k] is None else exact(record[k]) if k in NUMERIC[table]
              else json_numbers(record[k]) if k == 'stake_map' else record[k] for k in columns]
    sql = f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join("?" for _ in columns)})'
    if upsert:
        keys = {'week_info': 'week_id,ybs', 'user_info': 'account,week_id,ybs'}[table]
        sql += f' ON CONFLICT({keys}) DO UPDATE SET ' + ','.join(f'{k}=excluded.{k}' for k in columns)
    connection.execute(sql, values)


def prepare(store):
    if not store.rehearsal:
        raise RuntimeError('Prepare YBS state before activating the import')
    def create(connection):
        connection.execute('''CREATE TABLE ybs_checkpoints (
            ybs TEXT PRIMARY KEY,token TEXT UNIQUE NOT NULL,chain_id INTEGER NOT NULL,
            initial_block INTEGER NOT NULL,next_block INTEGER NOT NULL,previous_hash TEXT NOT NULL,
            CHECK(next_block >= initial_block))''')
        connection.execute('''CREATE TABLE ybs_events (
            event_key TEXT PRIMARY KEY,ybs TEXT NOT NULL,event_type TEXT NOT NULL,block INTEGER NOT NULL,
            FOREIGN KEY(ybs) REFERENCES ybs_checkpoints(ybs))''')
        connection.execute('''CREATE TABLE ybs_snapshot_progress (
            ybs TEXT NOT NULL,week_id INTEGER NOT NULL,end_block INTEGER NOT NULL,
            PRIMARY KEY(ybs,week_id))''')
        connection.execute("INSERT INTO _migration_meta VALUES ('ybs_schema_version','1')")
    store.write(create)


def ensure_ybs_schema():
    version = get_store().read(lambda c: c.execute("SELECT value FROM _migration_meta WHERE key='ybs_schema_version'").fetchone())
    if version is None or version[0] != '1':
        raise RuntimeError('YBS schema has not been explicitly prepared')


def checkpoint(connection, ybs):
    row = connection.execute('SELECT * FROM ybs_checkpoints WHERE ybs=?', (ybs.lower(),)).fetchone()
    if row is None:
        raise RuntimeError('YBS checkpoint missing; explicit cursor adoption is required')
    return dict(row)


def query_unique_accounts(token):
    return get_store().read(lambda c: [r[0] for r in c.execute('''SELECT account FROM stakes WHERE token=?
        UNION SELECT account FROM user_info WHERE token=?''', (token, token))])


def snapshot_has_baseline(ybs, week):
    return get_store().read(lambda c: c.execute(
        'SELECT 1 FROM ybs_snapshot_progress WHERE ybs=? AND week_id=?', (ybs, week)).fetchone() is not None)


def changed_accounts(token, start, end):
    def query(connection):
        deposit = connection.execute('''SELECT 1 FROM rewards WHERE token=? AND is_claim=0
            AND block>=? AND block<=? LIMIT 1''', (token, start, end)).fetchone()
        if deposit:
            return None  # A global reward change requires all accounts to refresh.
        return [row[0] for row in connection.execute('''SELECT account FROM stakes
            WHERE token=? AND block>=? AND block<=? UNION SELECT account FROM rewards
            WHERE token=? AND block>=? AND block<=?''', (token, start, end, token, start, end))]
    return get_store().read(query)


def get_latest_stake_recorded_for_token(token):
    return get_store().read(lambda c: c.execute('SELECT max(block) FROM stakes WHERE token=?', (token,)).fetchone()[0])


def get_highest_week_id_for_token(token):
    return get_store().read(lambda c: c.execute('SELECT max(week_id) FROM week_info WHERE token=?', (token,)).fetchone()[0])


def get_stake_bucket_amount(connection, token, unlock_week):
    row = connection.execute('SELECT net_amount FROM stake_buckets WHERE token=? AND unlock_week=?',
                             (token, unlock_week)).fetchone()
    return Decimal(row[0]) if row else Decimal(0)


def upsert_stake_bucket(connection, token, unlock_week, delta_amount):
    with localcontext() as context:
        context.prec = 100
        amount = get_stake_bucket_amount(connection, token, unlock_week) + Decimal(exact(delta_amount))
    connection.execute('''INSERT INTO stake_buckets (token,unlock_week,net_amount) VALUES (?,?,?)
        ON CONFLICT(token,unlock_week) DO UPDATE SET net_amount=excluded.net_amount''',
        (token, unlock_week, exact(amount)))


def apply_stake(connection, record, max_weeks):
    if record['is_stake']:
        upsert_stake_bucket(connection, record['token'], record['unlock_week'], record['amount'])
        return
    # Preserve the existing LIFO rule; realized stake has no pending bucket.
    with localcontext() as context:
        context.prec = 100
        remaining = Decimal(record['amount'])
        for week in range(record['week'] + max_weeks, record['week'], -1):
            amount = min(max(get_stake_bucket_amount(connection, record['token'], week), Decimal(0)), remaining)
            if amount:
                upsert_stake_bucket(connection, record['token'], week, -amount)
                remaining -= amount
            if not remaining:
                break


def commit_events(store, state, items, end, block_hash, max_weeks):
    def commit(connection):
        if checkpoint(connection, state['ybs']) != state:
            raise RuntimeError('YBS checkpoint advanced concurrently; retry the scan')
        for item in items:
            inserted = connection.execute('''INSERT INTO ybs_events VALUES (?,?,?,?)
                ON CONFLICT(event_key) DO NOTHING''',
                (item['key'], state['ybs'], item['event_type'], item['record']['block'])).rowcount
            if not inserted:
                continue
            table = 'stakes' if item['event_type'] in ('Staked', 'Unstaked') else 'rewards'
            insert(connection, table, item['record'])
            if table == 'stakes':
                apply_stake(connection, item['record'], max_weeks)
        connection.execute('UPDATE ybs_checkpoints SET next_block=?,previous_hash=? WHERE ybs=?',
                           (end + 1, block_hash, state['ybs']))
    store.write(commit)


def week_snapshot(ybs, week):
    return get_store().read(lambda c: dict(row) if (row := c.execute(
        'SELECT * FROM week_info WHERE ybs=? AND week_id=?', (ybs, week)).fetchone()) else None)


def publish_week(week_record, user_records, expected_end_block, removed_accounts=()):
    """All calculations must finish before publishing a week and its user updates."""
    def publish(connection):
        previous = connection.execute('SELECT end_block FROM week_info WHERE ybs=? AND week_id=?',
                                      (week_record['ybs'], week_record['week_id'])).fetchone()
        if (previous[0] if previous else None) != expected_end_block:
            raise RuntimeError('YBS week advanced concurrently; retry the refresh')
        if expected_end_block is not None and week_record['end_block'] < expected_end_block:
            raise RuntimeError('YBS week cannot move backwards')
        insert(connection, 'week_info', week_record, upsert=True)
        for record in user_records:
            if (record['week_id'], record['ybs']) != (week_record['week_id'], week_record['ybs']):
                raise ValueError('User record belongs to a different week')
            insert(connection, 'user_info', record, upsert=True)
        for account in removed_accounts:
            connection.execute('DELETE FROM user_info WHERE account=? AND ybs=? AND week_id=?',
                               (account, week_record['ybs'], week_record['week_id']))
        connection.execute('''INSERT INTO ybs_snapshot_progress VALUES (?,?,?)
            ON CONFLICT(ybs,week_id) DO UPDATE SET end_block=excluded.end_block''',
            (week_record['ybs'], week_record['week_id'], week_record['end_block']))
    get_store().write(publish)

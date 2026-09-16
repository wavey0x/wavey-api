"""Index YBS events in chain order with atomic rows, buckets, and progress."""

from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path

from utils import db as db_utils

CHUNK_SIZE = 5000
EVENT_TYPES = ('Staked', 'Unstaked', 'RewardsClaimed', 'RewardDeposited')


def hex_value(value):
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def bounded_logs(event, start, end):
    for first in range(start, end + 1, CHUNK_SIZE):
        yield from event.get_logs(fromBlock=first, toBlock=min(first + CHUNK_SIZE - 1, end))


def collect(w3, token, info, ranges, *, legacy_values=False):
    end = max(end for start, end in ranges.values())
    end_hash = hex_value(w3.eth.get_block(end)['hash'])
    blocks, receipts, items = {}, {}, []
    max_weeks = info['ybs'].MAX_STAKE_GROWTH_WEEKS(block_identifier=end)
    for event_type, (start, stop) in ranges.items():
        if start > stop:
            continue
        is_stake_event = event_type in ('Staked', 'Unstaked')
        contract = info['ybs'] if is_stake_event else info['rewards']
        event = getattr(contract.events, event_type)
        for log in bounded_logs(event, start, stop):
            number = log['blockNumber']
            if (not start <= number <= stop or log.get('removed', False)
                    or hex_value(log['address']) != hex_value(contract.address)):
                raise RuntimeError('RPC returned an invalid YBS log')
            if number not in blocks:
                blocks[number] = w3.eth.get_block(number)
            block = blocks[number]
            if hex_value(block['hash']) != hex_value(log['blockHash']):
                raise RuntimeError('YBS event block changed during collection')
            tx = hex_value(log['transactionHash'])
            if tx not in receipts:
                receipts[tx] = w3.eth.get_transaction_receipt(log['transactionHash'])
            receipt = receipts[tx]
            if (hex_value(receipt['blockHash']) != hex_value(log['blockHash'])
                    or hex_value(receipt['transactionHash']) != tx):
                raise RuntimeError('YBS receipt changed during collection')
            positions = [i for i, item in enumerate(receipt['logs']) if item['logIndex'] == log['logIndex']
                         and hex_value(item['address']) == hex_value(contract.address)]
            if len(positions) != 1:
                raise RuntimeError('YBS log is missing or ambiguous in its receipt')
            args = log['args']
            record = dict(ybs=info['ybs'].address, week=args['week'], timestamp=block['timestamp'],
                          date_str=datetime.fromtimestamp(block['timestamp'], timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                          txn_hash=log['transactionHash'].hex(), block=number, token=token)
            if is_stake_event:
                is_stake = event_type == 'Staked'
                raw = {'amount': args['amount'], 'new_weight': args['newUserWeight'],
                       'net_weight_change': args.get('weightAdded') or args.get('weightRemoved', 0)}
                record.update(account=args['account'], is_stake=is_stake,
                              unlock_week=args['week'] + max_weeks if is_stake else None)
            else:
                is_claim = event_type == 'RewardsClaimed'
                raw = {'amount': args['rewardAmount']}
                record.update(account=args['account'] if is_claim else args['depositor'],
                              reward_distributor=log['address'], is_claim=is_claim)
            # Retain the existing token-decimal convention for event records.
            record.update({key: db_utils.scaled(value, info['decimals']) for key, value in raw.items()})
            item = dict(key=f'1:{contract.address.lower()}:{tx}:{positions[0]}',
                        event_type=event_type, record=record, log_index=log['logIndex'])
            if legacy_values:
                item['legacy'] = {key: Decimal(str(value / 10 ** info['decimals'])) for key, value in raw.items()}
            items.append(item)
    if hex_value(w3.eth.get_block(end)['hash']) != end_hash:
        raise RuntimeError('YBS range changed during collection')
    return sorted(items, key=lambda item: (item['record']['block'], item['log_index'])), end_hash, max_weeks


def process_token_events(token, info, height, *, store=None, w3=None):
    if w3 is None:
        from brownie import web3
        w3 = web3
    store = store or db_utils.get_store()
    state = store.read(lambda connection: db_utils.checkpoint(connection, info['ybs'].address))
    if state['token'].lower() != token.lower() or state['chain_id'] != 1 or w3.eth.chain_id != 1:
        raise RuntimeError('YBS checkpoint chain or token does not match')
    height = min(height, w3.eth.get_block('finalized')['number'])
    while state['next_block'] <= height:
        start = state['next_block']
        if hex_value(w3.eth.get_block(start - 1)['hash']) != state['previous_hash']:
            raise RuntimeError('YBS checkpoint block changed; reconciliation required')
        end = min(start + CHUNK_SIZE - 1, height)
        items, block_hash, max_weeks = collect(w3, token, info, {kind: (start, end) for kind in EVENT_TYPES})
        if hex_value(w3.eth.get_block(start - 1)['hash']) != state['previous_hash']:
            raise RuntimeError('YBS checkpoint block changed during collection')
        db_utils.commit_events(store, state, items, end, block_hash, max_weeks)
        state = store.read(lambda connection: db_utils.checkpoint(connection, info['ybs'].address))


def matches_import(row, item):
    record = item['record']
    for key, expected in record.items():
        value = row[key]
        if key in item['legacy']:
            if value is None or Decimal(value) not in (expected, item['legacy'][key]):
                return False
        elif key in ('txn_hash', 'account', 'ybs', 'token', 'reward_distributor'):
            if value is None or hex_value(value) != hex_value(expected):
                return False
        elif value != expected:
            return False
    return True


def adopt_cursor(store, w3, token, info, cursor):
    """Adopt a frozen cursor only after checking the source/chain boundary."""
    if not store.rehearsal or w3.eth.chain_id != 1:
        raise RuntimeError('Adopt YBS cursors in an inactive mainnet import')
    streams = cursor.get(info['ybs'].address.lower(), {})
    positions = set(streams.values())
    if set(streams) != set(EVENT_TYPES) or len(positions) != 1:
        raise RuntimeError('YBS cursor streams are incomplete or disagree; reconcile before adoption')
    next_block = positions.pop()
    if type(next_block) is not int or next_block < 1:
        raise RuntimeError('YBS cursor must be a positive integer')
    if next_block - 1 > w3.eth.get_block('finalized')['number']:
        raise RuntimeError('Wait for the YBS source cursor to become finalized')
    rows_by_type, ranges = {}, {}
    for kind in EVENT_TYPES:
        stake = kind in ('Staked', 'Unstaked')
        table, flag = ('stakes', 'is_stake') if stake else ('rewards', 'is_claim')
        is_positive = kind in ('Staked', 'RewardsClaimed')
        rows = store.read(lambda connection: [dict(row) for row in connection.execute(
            f'''SELECT * FROM {table} WHERE lower(ybs)=lower(?) AND {flag}=? AND block=(
                SELECT max(block) FROM {table} WHERE lower(ybs)=lower(?) AND {flag}=?)''',
            (info['ybs'].address, is_positive, info['ybs'].address, is_positive))])
        start = rows[0]['block'] if rows else info['ybs_deploy_block']
        if start >= next_block:
            raise RuntimeError('YBS source rows are ahead of the cursor; reconcile the frozen copy')
        rows_by_type[kind] = rows
        ranges[kind] = (start, next_block - 1)
    items, block_hash, _ = collect(w3, token, info, ranges, legacy_values=True)
    for kind, rows in rows_by_type.items():
        events = [item for item in items if item['event_type'] == kind]
        if (any(not any(matches_import(row, item) for item in events) for row in rows)
                or any(not any(matches_import(row, item) for row in rows) for item in events)):
            raise RuntimeError('YBS source boundary differs from chain events; reconcile rows and buckets before adoption')

    def adopt(connection):
        connection.execute('INSERT INTO ybs_checkpoints VALUES (?,?,?,?,?,?)',
                           (info['ybs'].address.lower(), token, 1, next_block, next_block, block_hash))
        for item in items:
            connection.execute('INSERT INTO ybs_events VALUES (?,?,?,?)',
                               (item['key'], info['ybs'].address.lower(), item['event_type'], item['record']['block']))
        return {'token': token, 'next_block': next_block, 'validated_boundary_events': len(items)}
    return store.write(adopt)


def prepare_import(cursor_file):
    """Explicit Brownie migration entry point; never called by the scheduled job."""
    import os
    from brownie import web3
    from scripts.ybs_dash.main import populate_staker_info
    from utils.sqlite_store import Store
    cursor = json.loads(Path(cursor_file).read_text())
    store = Store(os.environ['YEARN_DB_PATH'], os.environ['YEARN_IMPORT_SHA256'], rehearsal=True)
    info_by_token = populate_staker_info()
    db_utils.prepare(store)
    for token, info in info_by_token.items():
        print(json.dumps(adopt_cursor(store, web3, token, info, cursor)))


def main():
    from brownie import web3
    from scripts.ybs_dash.main import populate_staker_info
    db_utils.ensure_ybs_schema()
    info_by_token = populate_staker_info()
    height = web3.eth.get_block('finalized')['number']
    for token, info in info_by_token.items():
        process_token_events(token, info, height, w3=web3)
    print(f'YBS event indexer completed through finalized block {height}')

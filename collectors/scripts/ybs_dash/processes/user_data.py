"""Publish consistent YBS snapshots after collecting all required chain data."""

from datetime import datetime, timezone
from decimal import Decimal, localcontext

from utils import db as db_utils


def main():
    from brownie import web3
    from utils.network import configure_timeouts
    configure_timeouts()
    from scripts.ybs_dash.main import populate_staker_info
    db_utils.ensure_ybs_schema()
    info_by_token = populate_staker_info()
    finalized = web3.eth.get_block('finalized')['number']
    for token, info in info_by_token.items():
        state = db_utils.get_store().read(lambda connection: db_utils.checkpoint(connection, info['ybs'].address))
        height = min(finalized, state['next_block'] - 1)
        if web3.eth.chain_id != state['chain_id'] or state['chain_id'] != 1:
            raise RuntimeError('YBS snapshot checkpoint chain does not match')
        if hex_value(web3.eth.get_block(state['next_block'] - 1)['hash']) != state['previous_hash']:
            raise RuntimeError('YBS snapshot checkpoint block changed; reconciliation required')
        fill_weeks(token, info, height, web3)


def hex_value(value):
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def fill_weeks(token, info, height, w3):
    from utils import utils as utilities
    ybs = info['ybs']
    current_week = int(ybs.getWeek(block_identifier=height))
    last_week = db_utils.get_highest_week_id_for_token(token)
    if last_week is None:
        last_week = int(ybs.getWeek(block_identifier=info['ybs_deploy_block']))
    # Complete the previous current week before moving forward. Older imported
    # weeks remain intact; a failed new week is retried because publication is atomic.
    for week in range(last_week, current_week + 1):
        end = height if week == current_week else min(height, utilities.get_week_end_block(ybs.address, week))
        refresh_week(token, info, week, end, w3, utilities)


def refresh_week(token, info, week, end, w3, utilities):
    ybs = info['ybs']
    previous = db_utils.week_snapshot(ybs.address, week)
    if previous and previous['end_block'] > end:
        return  # Wait for the event indexer to catch up with the imported snapshot.
    baseline = db_utils.snapshot_has_baseline(ybs.address, week)
    if previous and previous['end_block'] == end and baseline:
        return
    users = None
    if previous and baseline:
        users = db_utils.changed_accounts(token, previous['end_block'] + 1, end)
    if users is None:
        users = db_utils.query_unique_accounts(token)
    block_hash = hex_value(w3.eth.get_block(end)['hash'])
    with localcontext() as context:
        context.prec = 100
        decimals = int(ybs.decimals(block_identifier=end))
        max_weeks = int(ybs.MAX_STAKE_GROWTH_WEEKS(block_identifier=end))
        week_record = build_week_record(info, week, end, max_weeks, decimals, utilities)
        user_records, removed = build_user_records(users, info, week, end, max_weeks, decimals, utilities)
    if hex_value(w3.eth.get_block(end)['hash']) != block_hash:
        raise RuntimeError('YBS snapshot block changed during collection')
    db_utils.publish_week(week_record, user_records, previous['end_block'] if previous else None, removed)


def build_week_record(info, week, end, max_weeks, decimals, utilities):
    ybs = info['ybs']
    supply = db_utils.scaled(ybs.totalSupply(block_identifier=end), decimals)
    weight = db_utils.scaled(ybs.getGlobalWeightAt(week, block_identifier=end), decimals)
    start_ts = utilities.get_week_start_ts(ybs.address, week)
    end_ts = utilities.get_week_end_ts(ybs.address, week)
    return dict(week_id=week, token=info['token'].address, weight=weight, total_supply=supply,
                boost=weight / supply if supply else Decimal(0), ybs=ybs.address,
                start_ts=start_ts, end_ts=end_ts, start_block=utilities.get_week_start_block(ybs.address, week),
                end_block=end, start_time_str=datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d'),
                end_time_str=datetime.fromtimestamp(end_ts, timezone.utc).strftime('%Y-%m-%d'),
                stake_map=build_global_stake_map(ybs, week, end, max_weeks, decimals, utilities))


def build_user_records(users, info, week, end, max_weeks, decimals, utilities):
    ybs, rewards = info['ybs'], info['rewards']
    reward_token = info.get('reward_token')
    if reward_token is None:
        from brownie import Contract
        reward_token = Contract(rewards.rewardToken(block_identifier=end))
    reward_decimals = int(reward_token.decimals(block_identifier=end))
    records, removed = [], []
    for user in users:
        weight = db_utils.scaled(ybs.getAccountWeightAt(user, week, block_identifier=end), decimals)
        if weight == 0:
            removed.append(user)
            continue
        balance = db_utils.scaled(ybs.balanceOf(user, block_identifier=end), decimals)
        acct_data = ybs.accountData(user, block_identifier=end)
        stake_map = build_user_stake_map(ybs, user, acct_data, week, end, max_weeks, decimals, utilities)
        records.append(dict(account=user, week_id=week, token=info['token'].address,
                            weight=weight, balance=balance, boost=weight / balance if balance else Decimal(0),
                            stake_map=stake_map, rewards_earned=db_utils.scaled(
                                rewards.getClaimableAt(user, week, block_identifier=end), reward_decimals),
                            ybs=ybs.address, total_realized=stake_map['realized']))
    return records, removed


def build_global_stake_map(ybs, week, block, max_weeks, decimals, utilities):
    pending = {'realized': db_utils.scaled(ybs.totalSupply(block_identifier=block), decimals)}
    for index in range(max_weeks):
        target = week + 1 + index
        amount = db_utils.scaled(ybs.globalWeeklyToRealize(target, block_identifier=block)['weight'] * 2, decimals)
        pending[target] = dict(amount=amount, week_start_ts=utilities.get_week_start_ts(ybs.address, target), max_weeks=max_weeks)
        pending['realized'] -= amount
    return pending


def build_user_stake_map(ybs, user, acct_data, week, block, max_weeks, decimals, utilities):
    week_offset = week - acct_data['lastUpdateWeek']
    bitmap = acct_data['updateWeeksBitmap']
    bitstring = format(bitmap, '08b')[::-1][:-(max_weeks - 1)]
    pending = {'realized': db_utils.scaled(acct_data['realizedStake'] * 2, decimals)}
    for index, _ in enumerate(bitstring):
        target = week - week_offset + (len(bitstring) - 1 - index)
        amount = db_utils.scaled(ybs.accountWeeklyToRealize(user, target, block_identifier=block)['weight'] * 2, decimals)
        if target < week:
            pending['realized'] += amount
            amount = Decimal(0)
        pending[target] = dict(amount=amount, week_start_ts=utilities.get_week_start_ts(ybs.address, target), max_weeks=max_weeks)
    return pending

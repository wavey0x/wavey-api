from brownie import Contract, chain
from utils import utils as utilities
from constants import WEEK
import json

def build_data(token, staker_data, height, history):
    return {
        'weekly_data': get_week_data(token, staker_data, height, history),
        'ybs': staker_data['ybs'],
        'rewards': staker_data['rewards'],
        'utils': staker_data['utils'],
    }


def get_week_data(token, staker_data, height, history):
    week_data = dict(history)
    last_week_checked = max((int(week) for week in week_data), default=0)

    data = staker_data
    deploy_block = data['ybs_deploy_block']
    ybs = data['ybs']
    start_time = int(chain[deploy_block].timestamp / WEEK) * WEEK
    current_week = ybs.getWeek(block_identifier=height)
    ts = chain[height].timestamp
    current_week_start_time = int(ts / WEEK) * WEEK
    decimals = ybs.decimals()

    for i in range(0, 2_000):
        target_week_start_time = current_week_start_time - (WEEK * i)
        week = max(current_week - i, 0)
        if (
            last_week_checked > week or
            target_week_start_time < start_time
        ):
            break
        start_block = max(utilities.get_week_start_block(ybs.address, week), deploy_block)
        end_block = min(height, utilities.get_week_end_block(ybs.address, week))
        week_data[str(week)] = {
            'global_weight': ybs.getGlobalWeightAt(week, block_identifier=height) / 10**decimals,
            'global_balance': ybs.totalSupply(block_identifier=end_block) / 10**decimals,
            'start_time': target_week_start_time,
            'start_block': start_block,
        }
        strategy = data['strategy_data']['strategy']
        week_data[str(week)]['system_avg_boost'] = 0 if week_data[str(week)]['global_balance'] == 0 else week_data[str(week)]['global_weight'] / week_data[str(week)]['global_balance']
        week_data[str(week)]['strategy_weight'] = ybs.getAccountWeightAt(strategy, week, block_identifier=height) / 10 ** decimals
        week_data[str(week)]['strategy_balance'] = ybs.balanceOf(strategy, block_identifier=end_block) / 10 ** decimals
        week_data[str(week)]['strategy_boost'] = 0 if week_data[str(week)]['strategy_balance'] == 0 else week_data[str(week)]['strategy_weight'] / week_data[str(week)]['strategy_balance']

    return week_data

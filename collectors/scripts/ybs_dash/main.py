import os
import time
import json
from brownie import Contract, chain, network, web3
from dotenv import load_dotenv
from utils import utils as utilities
from config import YBS_REGISTRY
from utils.feeds import run_collection
from utils.feed_config import feed_config, DEPRECATED_YBS_TOKENS
from utils.feed_chain import finalized_boundary, advance
from scripts.ybs_dash.data_fetchers import (
    peg_data,
    strategy_data,
    token_price_data,
    processing_pipeline_data,
    ybs_data,
)

load_dotenv()

def collect(previous, state):
    end, end_hash = finalized_boundary(web3, state)
    staker_data = populate_staker_info()
    current_time = int(time.time())
    current_height = chain.height

    retained = state['retained']
    old_data = previous['data']
    for token, data in old_data.items():
        if token not in staker_data:
            retained.setdefault(token, {})['weekly_data'] = data['ybs_data']['weekly_data']
    for token, data in staker_data.items():
        data.update({
            'peg_data': peg_data.build_data(token, data, 10_000e18),
            'strategy_data': strategy_data.build_data(token, data),
            'pipeline_data': processing_pipeline_data.build_data(token, data),
        })
        history = retained.get(token, {}).get('weekly_data')
        if history is None:
            history = old_data.get(token, {}).get('ybs_data', {}).get('weekly_data', {})
        data['ybs_data'] = ybs_data.build_data(token, data, end, history)
        if token in retained:
            retained[token].pop('weekly_data', None)
            if not retained[token]:
                del retained[token]
        data['price_data'] = token_price_data.build_data(token, data)
        price = data['price_data'][data['reward_token'].address]['price']
        data['strategy_data']['swap_min_usd'] *= price
        data['strategy_data']['swap_max_usd'] *= price

    staker_data = {
        'data': staker_data,
        'last_update': current_time,
        'last_update_block': current_height,
    }

    staker_data_str = stringify_dicts(staker_data)
    return staker_data_str, advance(state, end, end_hash, web3)

def populate_staker_info():
    if not network.is_connected():
        network.connect("mainnet", launch_rpc=False)
    registry = Contract(YBS_REGISTRY)
    num_tokens = registry.numTokens()
    result = {}
    for i in range(num_tokens):
        token = registry.tokens(i)
        if token in DEPRECATED_YBS_TOKENS:
            continue
        deployment = registry.deployments(token)
        data = {
            'token': Contract(token),
            'ybs': Contract(deployment['yearnBoostedStaker']),
            'decimals': Contract(token).decimals(),
            'symbol': Contract(token).symbol(),
            'rewards': Contract(deployment['rewardDistributor']),
            'utils': Contract(deployment['utilities']),
            'ybs_deploy_block': utilities.contract_creation_block(deployment['yearnBoostedStaker']),
        }

        reward_token = Contract(data['rewards'].rewardToken())
        try:
            reward_token_underlying = Contract(reward_token.asset())
            data['reward_token_is_v2'] = False
        except:
            reward_token_underlying = Contract(reward_token.token())
            data['reward_token_is_v2'] = True

        data['reward_token'] = reward_token
        data['reward_token_underlying'] = reward_token_underlying

        result[token] = data

    return result

def stringify_dicts(data):
    if isinstance(data, dict):
        return {key: stringify_dicts(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [stringify_dicts(item) for item in data]
    elif isinstance(data, Contract):
        return data.address
    return data

def main():
    return run_collection('ybs', feed_config('ybs'), collect)


if __name__ == '__main__':
    main()

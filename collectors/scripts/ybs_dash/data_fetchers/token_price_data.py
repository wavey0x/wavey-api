from brownie import Contract, ZERO_ADDRESS, chain
import utils as utilities

def build_data(token, staker_data):
    reward_token_underlying = staker_data['reward_token_underlying'].address
    peg_token = staker_data['peg_data']['peg_token'].address

    token_list = [
        reward_token_underlying,
        peg_token,
        token
    ]
    prices = utilities.utils.get_prices(token_list)

    pps = staker_data['strategy_data']['price_per_share_autocompounder']
    autocompounder = staker_data['strategy_data']['autocompounder'].address
    prices[autocompounder] = prices[token] * pps

    reward_token = staker_data['strategy_data']['reward_token'].address
    pps = staker_data['strategy_data']['price_per_share_reward_token']
    prices[reward_token] = prices[reward_token_underlying] * pps

    price_data = {}
    for item in prices:
        price_data[item] = {}
        price_data[item]['logoURI'] = utilities.utils.get_token_logo_urls(item)
        price_data[item]['symbol'] = Contract(item).symbol()
        price_data[item]['price'] = prices[item]

    return price_data

from brownie import Contract

POOL_PER_TOKEN = {
    '0xFCc5c47bE19d06BF83eB04298b026F81069ff65b': '0x99f5acc8ec2da2bc0771c32814eff52b712de1e5', # yCRV
    '0xe3668873D944E4A949DA05fc8bDE419eFF543882': '0x69833361991ed76f9e8dbbcdf9ea1520febfb4a7', # yPRISMA
    '0x22222222aEA0076fCA927a3f44dc0B4FdF9479D6': '0x5Ee9606e5611Fd6CE14BD2BC12db70BD53dC9daA', # yYB
}

def build_data(token, staker_data, swap_size):
    pool = Contract(POOL_PER_TOKEN[token])
    zero_coin = pool.coins(0)
    peg_token = zero_coin if zero_coin != token else pool.coins(1)
    idx_ylocker_token = 1 if zero_coin == peg_token else 0
    idx_peg_token = 0 if idx_ylocker_token == 1 else 0
    output_amount = pool.get_dy(
        idx_ylocker_token,
        idx_peg_token,
        swap_size
    ) / 1e18

    swap_size /= 1e18
    return {
        'peg_token': Contract(peg_token),
        'swap_size': swap_size,
        'output_amount': output_amount,
        'peg': output_amount / swap_size,
        'balance_peg_token': pool.balances(idx_peg_token) / 1e18,
        'balance_ylocker_token': pool.balances(idx_ylocker_token) / 1e18,
        'pool': pool,
    }

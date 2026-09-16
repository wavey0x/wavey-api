from brownie import Contract, ZERO_ADDRESS, chain, interface
from constants import REGISTRY_V2, REGISTRY_V3, MAX_BPS_EXTENDED, DAY, YEAR

# (token, vault, is_v2)
VAULT_PER_TOKEN = {
    '0x22222222aEA0076fCA927a3f44dc0B4FdF9479D6': ('0x1F6f16945e395593d8050d6Cc33e4328a515B648', False), # yYB
}

def build_data(token, staker_data):
    decimals = staker_data['decimals']
    is_v2, vault = lookup_autocompounder(token)
    strategy = lookup_strategy(vault, is_v2)
    reward_token_decimals = 18
    reward_token = ZERO_ADDRESS
    if strategy != ZERO_ADDRESS:
        strategy = interface.IYBSStrategy(strategy)
        reward_token, unsold_rewards = fetch_unsold_rewards(strategy)
        swap_thresholds = strategy.swapThresholds()
        reward_token_decimals = reward_token.decimals()
        profit_release_data = get_profit_release_data(vault, is_v2, decimals)
        trigger = strategy.harvestTrigger(0) if is_v2 else strategy.reportTrigger(strategy.address)[0]
    else:
        trigger = 0
    return {
        'is_v2': is_v2,
        'autocompounder': vault,
        'strategy': strategy,
        'unsold_rewards': unsold_rewards / 10 ** reward_token_decimals,
        'profit_release_data': profit_release_data,
        'swap_min': swap_thresholds['min'] / 10 ** decimals,
        'swap_max': swap_thresholds['max'] / 10 ** decimals,
        'swap_min_usd': swap_thresholds['min'] / 10 ** decimals,
        'swap_max_usd': swap_thresholds['max'] / 10 ** decimals,
        'reward_token_decimals': reward_token_decimals,
        'reward_token': reward_token,
        'price_per_share_autocompounder': vault.pricePerShare() / 10 ** decimals,
        'price_per_share_reward_token': reward_token.pricePerShare() / 10 ** reward_token_decimals,
        'claimable_rewards': staker_data['rewards'].getClaimable(strategy) / 10 ** reward_token_decimals,
        'harvest_trigger': trigger,
        'credit': Contract(token).balanceOf(vault) / 10 ** decimals,
    }

def fetch_unsold_rewards(strategy):
    reward_token = Contract(strategy.rewardToken())
    return reward_token, reward_token.balanceOf(strategy)

def lookup_autocompounder(token):
    if token in VAULT_PER_TOKEN:
        vault, is_v2 = VAULT_PER_TOKEN[token]
        return is_v2, Contract(vault)
    registry = None
    try:
        is_v2 = True
        registry = Contract('0xaF1f5e1c19cB68B30aAD73846eFfDf78a5863319')
        return is_v2, Contract(registry.latestVault(token))
    except:
        is_v2 = False
        registry = Contract('0xff31A1B020c868F6eA3f61Eb953344920EeCA3af')
        return is_v2, Contract(registry.getEndorsedVaults(token)[0])

def lookup_strategy(vault, is_v2):
    dr = 0
    if is_v2:
        for i in range(20):
            s = vault.withdrawalQueue(i)
            if s == ZERO_ADDRESS:
                break
            else:
                if vault.strategies(s)['debtRatio'] > dr:
                    return Contract(s)

    queue = list(vault.get_default_queue())
    strategy = ZERO_ADDRESS
    for s in queue:
        current_debt = vault.strategies(s)['current_debt']
        if current_debt > dr:
            strategy = s
            dr = current_debt
    return strategy

def get_profit_release_data(vault, is_v2, decimals):
    """
    Calculates profit release information based on vault type and status.

    Parameters:
    - vault (Contract): The vault contract instance.
    - is_v2 (bool): Indicates if the vault version is 2.

    Returns:
    - dict: Contains `unlock_rate` (float) representing the number of tokens released per second,
            `unlock_date` (int) showing the epoch time when profits are fully unlocked,
            and `apr` (float) representing the annual percentage rate based on the unlocking rate.

    The function computes different return values based on the vault's version and current time relative
    to the report and unlock dates. For V2 vaults or when profits are fully unlocked, all return values are zero.
    """
    if not is_v2:
        unlock_date = vault.fullProfitUnlockDate()
        if chain.time() > unlock_date:
            return {
                'unlock_rate': 0,
                'unlock_date': 0,
                'apr': 0,
            }
        unlock_rate = vault.profitUnlockingRate()
        tokens_per_second = unlock_rate / MAX_BPS_EXTENDED / 10 ** decimals
        total_assets = vault.totalAssets()
        apr = (tokens_per_second * YEAR) / total_assets
        return {
            'unlock_rate': tokens_per_second,
            'unlock_date': unlock_date,
            'apr': apr,
        }

    locked_profit = vault.lockedProfit()
    unlock_period = int(1e18 / vault.lockedProfitDegradation())
    last_report = vault.lastReport()
    unlock_date = last_report + unlock_period
    ts = chain.time()
    if ts > unlock_date:
        return {
            'unlock_rate': 0,
            'unlock_date': 0,
            'apr': 0,
        }

    tokens_per_second = locked_profit / unlock_period / 10 ** decimals
    total_assets = vault.totalAssets() / 10 ** decimals
    apr = (tokens_per_second * YEAR) / total_assets
    return {
        'unlock_rate': tokens_per_second,
        'unlock_date': unlock_date,
        'apr': apr,
    }

def fetch_last_harvest(contract_address):
    return

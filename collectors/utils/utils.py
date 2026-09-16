from brownie import ZERO_ADDRESS, Contract, web3, accounts, chain
import requests, json
from datetime import datetime
from functools import lru_cache
from utils.cache import memory
import time

DAY = 60 * 60 * 24
WEEK = DAY * 7

def get_week_by_ts(contract, ts):
    contract = Contract(contract)
    block = contract_creation_block(contract.address)
    start_week = contract.getWeek(block_identifier=block)
    first_week_start_ts = get_week_start_ts(contract.address, week_number=start_week)
    if ts < first_week_start_ts:
        raise Exception("timestamp is before protocol launch")
    diff = ts - first_week_start_ts
    return diff // WEEK

@memory.cache()
def get_launch_week(contract):
    deploy_block = contract_creation_block(contract)
    deploy_ts = chain[deploy_block].timestamp
    current_week_start = (chain.time() / WEEK) * WEEK
    offset = current_week_start - deploy_ts
    num_weeks = offset / WEEK
    contract = Contract(contract)
    return contract.getWeek() - num_weeks

@memory.cache()
def get_week_start_block(contract, week_number=0):
    ts = get_week_start_ts(contract, week_number)
    return closest_block_after_timestamp(ts)

@memory.cache()
def get_week_start_ts(contract, week_number=0):
    contract = Contract(contract)
    current_week = contract.getWeek()
    offset = abs(current_week - week_number)
    current_week_start_ts = int(chain.time() / WEEK) * WEEK
    if week_number <= current_week:
        return int(current_week_start_ts - (WEEK * offset))
    else:
        return int(current_week_start_ts + (WEEK * offset))

def get_week_end_block(contract, week_number=0):
    contract = Contract(contract)
    current_week = contract.getWeek()
    if week_number == current_week:
        return chain.height - 1
    return get_past_week_end_block(contract.address, week_number)

@memory.cache()
def get_past_week_end_block(contract, week_number=0):
    ts = get_week_start_ts(contract, week_number) + WEEK
    return closest_block_after_timestamp(ts) - 1

@memory.cache()
def get_week_end_ts(contract, week_number=0):
    """
        This will always be precise. Never returns chain.time()
    """
    start = get_week_start_ts(contract, week_number + 1)
    return start - 1

def block_to_date(b):
    time = chain[b].timestamp
    return datetime.fromtimestamp(time)

def closest_block_after_timestamp(timestamp: int) -> int:
    height = chain.height
    lo, hi = 0, height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        if get_block_timestamp(mid) > timestamp:
            hi = mid
        else:
            lo = mid

    if get_block_timestamp(hi) < timestamp:
        raise Exception("timestamp is in the future")

    return hi

@memory.cache()
def closest_block_before_timestamp(timestamp: int) -> int:
    return closest_block_after_timestamp(timestamp) - 1

def get_block_timestamp(height):
    return chain[height].timestamp

def get_block_before_timestamp(timestamp: int) -> int:
    return get_block_after_timestamp(timestamp) - 1

def get_block_after_timestamp(timestamp: int) -> int:
    return _closest_block_after_timestamp(web3.eth.chain_id, timestamp)

def _closest_block_after_timestamp(chain_id, timestamp: int) -> int:
    height = web3.eth.block_number
    lo, hi = 0, height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        if get_block_timestamp(mid) > timestamp:
            hi = mid
        else:
            lo = mid

    if get_block_timestamp(hi) < timestamp:
        raise Exception("timestamp is in the future")
    # print(f'Chain ID: {chain_id} {hi}')
    return hi

def timestamp_to_date_string(ts):
    return datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")

def timestamp_to_string(ts):
    dt = datetime.utcfromtimestamp(ts).strftime("%m/%d/%Y, %H:%M:%S")
    return dt

def get_prices(tokens=[]):
    # Query DefiLlama for all of our coin prices
    coins = ','.join(f'ethereum:{k}' for k in tokens)
    url = f'https://coins.llama.fi/prices/current/{coins}?searchWidth=40h'
    response = requests.get(url, timeout=30).json()['coins']
    response = {key.replace('ethereum:', ''): value for key, value in response.items()}
    prices = {}
    for t in tokens:
        if t in response:
            prices[t] = response[t]['price']
    return prices

# Global cache for CoinGecko tokens
_COINGECKO_TOKENS = None

@memory.cache()
def get_coingecko_tokens():
    """Fetch CoinGecko token list with caching and retry logic"""
    global _COINGECKO_TOKENS
    if _COINGECKO_TOKENS is not None:
        return _COINGECKO_TOKENS

    url = "https://tokens.coingecko.com/uniswap/all.json"
    max_retries = 3
    base_delay = 2

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=5)

            if response.status_code == 429:
                delay = base_delay * (2 ** attempt)
                print(f"Rate limited by CoinGecko, waiting {delay} seconds...")
                time.sleep(delay)
                continue

            if response.status_code != 200:
                print(f"Warning: CoinGecko request failed with status {response.status_code}")
                return None

            _COINGECKO_TOKENS = response.json()
            return _COINGECKO_TOKENS

        except (requests.exceptions.RequestException, requests.exceptions.JSONDecodeError) as e:
            print(f"Warning: Failed to fetch CoinGecko tokens: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
                continue
            return None

    return None

def get_token_logo_url(token_address):
    """Wrapper to ensure cached function sees simple string inputs."""
    return _get_token_logo_url_cached(str(token_address))


@memory.cache()
def _get_token_logo_url_cached(token_address: str):
    """Get token logo URL from CoinGecko or SmolDapp fallback"""
    try:
        # First try CoinGecko using cached data
        if token_address not in [
            '0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E', # crvusd
            '0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0', # wsteth
        ]:
            tokens = get_coingecko_tokens()
            if tokens and 'tokens' in tokens:
                for token in tokens['tokens']:
                    if token['address'].lower() == token_address.lower():
                        return token['logoURI']

        # Fallback to SmolDapp token assets
        return f"https://assets.smold.app/api/token/1/{token_address}/logo-32.png"

    except requests.exceptions.RequestException as e:
        print(f"Warning: Request failed for token {token_address}: {str(e)}")
        return None

    return None


def get_token_logo_urls(token_address):
    return _get_token_logo_urls_cached(str(token_address))


TOKEN_LOGO_OVERRIDES = {
    '0x22222222aEA0076fCA927a3f44dc0B4FdF9479D6': 'https://etherscan.io/token/images/yearn_yyb.png', # yYB
}

@memory.cache()
def _get_token_logo_urls_cached(token_address):
    if token_address in TOKEN_LOGO_OVERRIDES:
        return TOKEN_LOGO_OVERRIDES[token_address]
    url = 'https://raw.githubusercontent.com/SmolDapp/tokenLists/main/lists/coingecko.json'
    data = requests.get(url, timeout=30).json()
    logo_url = ''
    for d in data['tokens']:
        if token_address == d['address']:
            logo_url = d['logoURI']
            return logo_url
    # Fallback to SmolDapp
    return f"https://assets.smold.app/api/token/1/{token_address}/logo-128.png"

@memory.cache()
def contract_creation_block(address):
    """
    Find contract creation block using binary search.
    NOTE Requires access to historical state. Doesn't account for CREATE2 or SELFDESTRUCT.
    """
    lo = 0
    hi = end = chain.height

    while hi - lo > 1:
        mid = lo + (hi - lo) // 2
        code = web3.eth.get_code(address, block_identifier=mid)
        if code:
            hi = mid
        else:
            lo = mid


    return hi if hi != end else None

def get_logs_chunked(contract, event_name, start_block=0, end_block=0, chunk_size=100_000):
    try:
        event = getattr(contract.events, event_name)
    except Exception as e:
        print(f'Contract has no event by the name {event_name}', e)

    if start_block == 0:
        start_block = contract_creation_block(contract.address)
    if end_block == 0:
        end_block = chain.height

    logs = []
    while start_block < end_block:
        logs += event.get_logs(fromBlock=start_block, toBlock=min(end_block, start_block + chunk_size))
        start_block += chunk_size

    return logs

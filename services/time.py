import datetime
import pytz, time
from flask import Flask, request, jsonify
from dateutil.relativedelta import relativedelta
from functools import lru_cache
from services.web3_services import setup_web3

# Initialize web3
web3 = setup_web3()

@lru_cache(maxsize=1024)
def get_block_timestamp(height):
    """Get timestamp for a specific block height"""
    return web3.eth.get_block(height)['timestamp']

def closest_block_after_timestamp(timestamp: int) -> int:
    """Find the closest block after or at the given timestamp"""
    return _closest_block_after_timestamp(web3.eth.chain_id, timestamp)

@lru_cache(maxsize=1024)
def _closest_block_after_timestamp(chain_id, timestamp: int) -> int:
    """Binary search to find closest block after timestamp"""
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

    return hi

@lru_cache(maxsize=1024)
def closest_block_before_timestamp(timestamp: int) -> int:
    """Find the closest block before or at the given timestamp"""
    return closest_block_after_timestamp(timestamp) - 1

def convert_timestamp(unix_timestamp):
    # Handle None or 'now' - default to current timestamp
    if unix_timestamp is None or (isinstance(unix_timestamp, str) and unix_timestamp.lower() == 'now'):
        unix_timestamp = int(time.time())
    else:
        try:
            unix_timestamp = int(unix_timestamp)
        except ValueError as e:
            return jsonify({"error": 'Invalid input. Please pass a timestamp as integer.'}), 400

    # Convert Unix timestamp to UTC
    utc_time = datetime.datetime.utcfromtimestamp(unix_timestamp).replace(tzinfo=pytz.utc)

    # Calculate relative time
    now = datetime.datetime.now(pytz.utc)
    if now > utc_time:
        delta = relativedelta(now, utc_time)
        relative_time = f"{abs(delta.years)} years, {abs(delta.months)} months, {abs(delta.days)} days, " \
                        f"{abs(delta.hours)} hours, {abs(delta.minutes)} minutes ago"
    else:
        delta = relativedelta(utc_time, now)
        relative_time = f"in {delta.years} years, {delta.months} months, {delta.days} days, " \
                        f"{delta.hours} hours, {delta.minutes} minutes"

    response = {
        'unix_timestamp': unix_timestamp,
        'utc_time': utc_time,
        # 'utc_time_string': utc_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'relative_time': relative_time
    }

    # Add Ethereum block information
    try:
        eth_block = closest_block_before_timestamp(unix_timestamp)
        response['ethereum_block'] = eth_block
    except Exception as e:
        # If block lookup fails (e.g., timestamp in future, RPC issue), don't break the response
        response['ethereum_block'] = None
        response['ethereum_block_error'] = str(e)

    return response
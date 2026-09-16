import os
from brownie import chain, network
from joblib import Memory

def _resolve_cache_dir():
    override = os.getenv("YBS_CACHE_DIR")
    if override:
        return override
    if network.is_connected():
        return f"cache/{chain.id}"
    chain_id = os.getenv("CHAIN_ID")
    if chain_id:
        return f"cache/{chain_id}"
    return "cache/unknown"

memory = Memory(_resolve_cache_dir(), verbose=0)

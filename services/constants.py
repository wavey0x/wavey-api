"""
Constants used throughout the project
"""
import os

# Provider wallet addresses
PROVIDER_WALLETS = {
    "yearn": "0xF147b8125d2ef93FB6965Db97D6746952a133934",
    "stakedao": "0x52f541764E6e90eeBc5c21Ff570De0e2D63766B6",
    "convex": "0x989AEb4d175e16225E39E87d0D97A3360524AD80",
}

# Maximum boost value
MAX_BOOST = 2.5
PER_MAX_BOOST = 1.0 / MAX_BOOST

# Default web3 provider from environment variable
DEFAULT_WEB3_PROVIDER = os.environ.get("MAINNET_RPC", "http://localhost:8545") 
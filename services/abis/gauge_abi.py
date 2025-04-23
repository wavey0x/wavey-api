"""
ABI definitions for gauge contracts
"""

# Gauge ABI with common functions
GAUGE_ABI = [
    {"name": "working_balances", "inputs": [{"type": "address", "name": "addr"}], 
     "outputs": [{"type": "uint256", "name": ""}], "stateMutability": "view", "type": "function"},
    {"name": "balanceOf", "inputs": [{"type": "address", "name": "addr"}], 
     "outputs": [{"type": "uint256", "name": ""}], "stateMutability": "view", "type": "function"},
    {"name": "factory", "inputs": [], "outputs": [{"type": "address", "name": ""}], 
     "stateMutability": "view", "type": "function"},
    {"name": "lp_token", "inputs": [], "outputs": [{"type": "address", "name": ""}], 
     "stateMutability": "view", "type": "function"},
    {"name": "totalSupply", "inputs": [], "outputs": [{"type": "uint256", "name": ""}],
     "stateMutability": "view", "type": "function"}
] 
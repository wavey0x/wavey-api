from web3 import Web3
from flask import current_app
import os

def setup_web3(provider_url=None):
    """
    Set up and return a Web3 instance
    
    Args:
        provider_url: Optional custom provider URL
        
    Returns:
        Web3 instance
    """
    # Priority 1: Explicitly provided URL
    if provider_url:
        return Web3(Web3.HTTPProvider(provider_url))
    
    # Priority 2: Environment variable
    env_provider = os.environ.get("MAINNET_RPC")
    if env_provider:
        return Web3(Web3.HTTPProvider(env_provider))
    
    # Priority 3: Flask app config if available
    try:
        infura_id = current_app.config.get('WEB3_INFURA_PROJECT_ID')
        if infura_id:
            return Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_id}'))
    except RuntimeError:
        # Not in Flask context
        pass
    
    # Priority 4: Infura ID from environment variable
    infura_id = os.environ.get('WEB3_INFURA_PROJECT_ID')
    if infura_id:
        return Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_id}'))
    
    # Fallback warning
    print("WARNING: Using default Web3 provider. Set WEB3_PROVIDER_URL environment variable for production.")
    return Web3(Web3.HTTPProvider("http://localhost:8545"))

# Helper function to get contract
def get_contract(web3, address, abi):
    """
    Get a contract instance
    
    Args:
        web3: Web3 instance
        address: Contract address
        abi: Contract ABI
        
    Returns:
        Contract instance
    """
    return web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
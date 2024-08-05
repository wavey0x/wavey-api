from web3 import Web3
from flask import current_app

def setup_web3():
    infura_id = current_app.config['WEB3_INFURA_PROJECT_ID']
    # Setting up Web3 connection and functionalities
    web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{infura_id}'))
    return web3
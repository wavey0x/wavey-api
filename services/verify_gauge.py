from web3 import Web3
import config
from .web3_services import setup_web3

GAUGE_ABI = [
    {"constant": True, "inputs": [], "name": "factory", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "lp_token", "outputs": [{"name": "", "type": "address"}], "type": "function"},
]

FACTORY_ABI = [
    {"constant": True, "inputs": [{"name": "pool", "type": "address"}], "name": "get_gauge", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "gauge", "type": "address"}], "name": "is_valid_gauge", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]

TRUSTED_FACTORIES = [
    '0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf', # Regular
    '0xabC000d88f23Bb45525E447528DBF656A9D55bf5', # Bridge factory
    '0xeF672bD94913CB6f1d2812a6e18c1fFdEd8eFf5c', # root / child gauge factory for fraxtal
    '0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F', # CurveTwocryptoFactory
    '0x306A45a1478A000dC701A6e1f7a569afb8D9DCD6', # Root liquidity gauge factory
]

def get_contract_function_output(web3, address, abi, function_name, args=[]):
    contract = web3.eth.contract(address=address, abi=abi)
    function = getattr(contract.functions, function_name)
    return function(*args).call()

def verify_gauge(request):
    response = {'is_valid': False, 'message': ''}
    address = request.args.get('a') or request.args.get('address')
    if not address or address == '':
        response['message'] = 'No address parameter given.'
        return response
    
    web3 = setup_web3()

    if web3.is_address(address) == False:
        response['message'] = 'Invalid Ethereum address.'
        return response

    address = web3.to_checksum_address(address)

    if not is_valid_contract(web3, address):
        response['message'] = "Supplied address is not a valid contract."
        return response

    # Validate factory
    try:
        factory_address = get_contract_function_output(web3, address, GAUGE_ABI, 'factory')
    except:
        response['message'] = "Contract call to discover factory reverted. Ensure you provide a factory depolyed gauge."
        response['is_valid'] = False
        return response
    if factory_address not in TRUSTED_FACTORIES:
        response['message'] = "Factory used to deploy this is not found on trusted list."
        response['is_valid'] = False
        return response

    is_lp_gauge = True
    try:
        lp_token_address = get_contract_function_output(web3, address, GAUGE_ABI, 'lp_token')
    except:
        is_lp_gauge = False

    if is_lp_gauge:
        try:
            gauge_address = get_contract_function_output(web3, factory_address, FACTORY_ABI, 'get_gauge', args=[lp_token_address])
        except:
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            response['is_valid'] = False
            return response
    else:
        try:
            print(factory_address)
            is_valid_gauge = get_contract_function_output(web3, factory_address, FACTORY_ABI, 'is_valid_gauge', args=[address])
        except:
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            response['is_valid'] = False
            return response
        if is_valid_gauge:
            response['message'] = "This is a verified factory deployed gauge."
            response['is_valid'] = True
            return response
        else:
            response['message'] = "The factory reports this gauge as invalid."
            response['is_valid'] = False
            return response

    if gauge_address != address:
        response['message'] = "Factory address for this pool does not match supplied address."
    else:
        response['is_valid'] = True
        response['message'] = "This is a verified factory deployed gauge."
    return response

def is_valid_contract(web3, address):
    return web3.eth.get_code(address) != '0x0'
import logging
import time
from .web3_services import setup_web3, get_contract
from .multicall import batch_calls

# Configure logger
logger = logging.getLogger(__name__)

GAUGE_ABI = [
    {"constant": True, "inputs": [], "name": "factory", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "lp_token", "outputs": [{"name": "", "type": "address"}], "type": "function"},
]

FACTORY_ABI = [
    {"constant": True, "inputs": [{"name": "pool", "type": "address"}], "name": "get_gauge", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "gauge", "type": "address"}], "name": "is_valid_gauge", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]

TRUSTED_FACTORIES = [
    '0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf',  # Regular
    '0xabC000d88f23Bb45525E447528DBF656A9D55bf5',  # Bridge factory
    '0xeF672bD94913CB6f1d2812a6e18c1fFdEd8eFf5c',  # root / child gauge factory for fraxtal
    '0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F',  # CurveTwocryptoFactory
    '0x306A45a1478A000dC701A6e1f7a569afb8D9DCD6',  # Root liquidity gauge factory
]

def verify_gauge(request):
    start_time = time.time()
    response = {'is_valid': False, 'message': ''}

    address = request.args.get('a') or request.args.get('address')
    if not address or address == '':
        response['message'] = 'No address parameter given.'
        return response
    
    web3 = setup_web3()

    if not web3.is_address(address):
        response['message'] = 'Invalid Ethereum address.'
        return response

    address = web3.to_checksum_address(address)
    logger.info(f"Starting verification for gauge address: {address}")

    # Check if contract exists
    if not is_valid_contract(web3, address):
        response['message'] = "Supplied address is not a valid contract."
        elapsed = time.time() - start_time
        logger.info(f"Gauge verification failed (not a contract) in {elapsed:.3f}s")
        return response

    # Direct function call for factory - critical part that must work
    try:
        # Use get_contract to make a direct call for the factory address
        contract = get_contract(web3, address, GAUGE_ABI)
        factory_address = contract.functions.factory().call()
        logger.info(f"Factory address: {factory_address}")
        
        if factory_address not in TRUSTED_FACTORIES:
            response['message'] = "Factory used to deploy this is not found on trusted list."
            elapsed = time.time() - start_time
            logger.info(f"Gauge verification failed (untrusted factory: {factory_address}) in {elapsed:.3f}s")
            return response
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error getting factory address: {e}")
        response['message'] = "Contract call to discover factory reverted. Ensure you provide a factory deployed gauge."
        return response

    # Try to determine if this is an LP gauge
    is_lp_gauge = True
    try:
        # Direct call for lp_token
        contract = get_contract(web3, address, GAUGE_ABI)
        lp_token_address = contract.functions.lp_token().call()
        logger.info(f"LP token address: {lp_token_address}")
    except Exception as e:
        logger.info(f"Not an LP gauge (lp_token call failed): {e}")
        is_lp_gauge = False
        lp_token_address = None

    logger.info(f"Is LP gauge: {is_lp_gauge}")
    
    # Handle LP gauges vs non-LP gauges
    if is_lp_gauge:
        try:
            # Direct call for get_gauge
            factory_contract = get_contract(web3, factory_address, FACTORY_ABI)
            gauge_address = factory_contract.functions.get_gauge(lp_token_address).call()
            logger.info(f"Factory's gauge address for LP token: {gauge_address}")
            
            # Case-insensitive address comparison
            if gauge_address.lower() != address.lower():
                response['message'] = f"Factory address for this pool does not match supplied address. Expected {gauge_address}, got {address}"
                elapsed = time.time() - start_time
                logger.info(f"Gauge verification failed (address mismatch) in {elapsed:.3f}s")
                return response
            
            response['is_valid'] = True
            response['message'] = "This is a verified factory deployed LP gauge."
            elapsed = time.time() - start_time
            logger.info(f"Gauge verification succeeded (LP gauge) in {elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error validating LP gauge: {e}")
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            return response
    else:
        try:
            # Direct call for is_valid_gauge
            factory_contract = get_contract(web3, factory_address, FACTORY_ABI)
            is_valid_gauge = factory_contract.functions.is_valid_gauge(address).call()
            logger.info(f"Factory reports is_valid_gauge: {is_valid_gauge}")
            
            if is_valid_gauge:
                response['is_valid'] = True
                response['message'] = "This is a verified factory deployed gauge."
                elapsed = time.time() - start_time
                logger.info(f"Gauge verification succeeded (non-LP gauge) in {elapsed:.3f}s")
            else:
                response['message'] = "The factory reports this gauge as invalid."
                elapsed = time.time() - start_time
                logger.info(f"Gauge verification failed (factory reports invalid) in {elapsed:.3f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error checking is_valid_gauge: {e}")
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            return response

    # Add timing information to response
    response["timing"] = {
        "total_seconds": time.time() - start_time
    }
    
    return response

def verify_gauge_by_address(address):
    """
    Verify a gauge by directly providing its address
    
    Args:
        address: The gauge address to verify
        
    Returns:
        Dictionary with verification result
    """
    class DummyRequest:
        def __init__(self, address):
            class Args:
                def get(self, param_name):
                    if param_name in ['a', 'address']:
                        return address
                    return None
            self.args = Args()
    
    dummy_request = DummyRequest(address)
    return verify_gauge(dummy_request)

def is_valid_contract(web3, address):
    """Check if an address is a valid contract"""
    try:
        code = web3.eth.get_code(address)
        return code != '0x' and code != '0x0'
    except Exception as e:
        logger.error(f"Error checking if {address} is a contract: {e}")
        return False
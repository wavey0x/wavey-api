"""
Multicall functionality to batch blockchain calls
"""
from typing import List, Dict, Any, Tuple
from web3 import Web3
import time
import logging
from .abis.multicall import MULTICALL_ABI

# Configure logger
logger = logging.getLogger(__name__)

# Multicall contract address on Ethereum mainnet
MULTICALL_ADDRESS = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"

def batch_calls(web3: Web3, calls: List[Tuple[str, str, List]], abi: List[Dict] = None) -> List[Any]:
    """
    Execute multiple contract calls in a single transaction using Multicall
    
    Args:
        web3: Web3 instance
        calls: List of tuples (contract_address, function_name, function_args)
        abi: ABI to use for contract calls (defaults to gauge ABI)
        
    Returns:
        List of decoded return values
    """
    start_time = time.time()
    logger.info(f"Starting multicall batch with {len(calls)} calls")
    
    # If there are no calls, return an empty list
    if not calls:
        return []
    
    # Separate calls that might fail (for gauge verification)
    reliable_calls = []
    reliable_indices = []
    potentially_failing_calls = []
    potentially_failing_indices = []
    
    # If function name is lp_token, it might fail
    for i, (address, function_name, args) in enumerate(calls):
        if function_name == "lp_token":
            potentially_failing_calls.append((address, function_name, args))
            potentially_failing_indices.append(i)
        else:
            reliable_calls.append((address, function_name, args))
            reliable_indices.append(i)
    
    multicall = web3.eth.contract(address=web3.to_checksum_address(MULTICALL_ADDRESS), abi=MULTICALL_ABI)
    
    # If no ABI provided, import Gauge ABI
    if abi is None:
        from .abis.gauge_abi import GAUGE_ABI
        abi = GAUGE_ABI
    
    # Initialize results array with None values
    all_results = [None] * len(calls)
    
    # Process reliable calls with multicall
    if reliable_calls:
        try:
            reliable_results = _execute_multicall_batch(web3, multicall, reliable_calls, abi)
            # Map results back to their original positions
            for i, result in enumerate(reliable_results):
                original_index = reliable_indices[i]
                all_results[original_index] = result
        except Exception as e:
            logger.error(f"Error in multicall reliable batch: {e}")
            # On failure, leave the results as None
    
    # Process potentially failing calls individually
    for i, (address, function_name, args) in enumerate(potentially_failing_calls):
        try:
            contract = web3.eth.contract(address=web3.to_checksum_address(address), abi=abi)
            function = getattr(contract.functions, function_name)
            result = function(*args).call()
            original_index = potentially_failing_indices[i]
            all_results[original_index] = result
        except Exception as e:
            logger.debug(f"Expected potential failure: {function_name} on {address}: {e}")
            # Leave result as None for failed calls
    
    total_elapsed = time.time() - start_time
    logger.info(f"Completed multicall batch ({len(calls)} calls) in {total_elapsed:.3f}s")
    return all_results

def _execute_multicall_batch(web3, multicall_contract, calls, abi):
    """Helper function to execute a batch of calls via multicall"""
    # Prepare calls for multicall
    multicall_calls = []
    contracts = {}
    function_abis = {}
    
    # Cache contracts to avoid recreating them
    for contract_address, function_name, function_args in calls:
        if contract_address not in contracts:
            contracts[contract_address] = web3.eth.contract(
                address=web3.to_checksum_address(contract_address),
                abi=abi
            )
            
        # Cache function ABI for later decoding
        if function_name not in function_abis:
            function_abis[function_name] = next((item for item in abi if item.get("name") == function_name), None)
            
        # Encode function call
        function = getattr(contracts[contract_address].functions, function_name)
        call_data = function(*function_args).build_transaction({"gas": 0, "gasPrice": 0})["data"]
        
        multicall_calls.append({
            "target": web3.to_checksum_address(contract_address),
            "callData": call_data
        })
    
    # Execute multicall
    _, result = multicall_contract.functions.aggregate(multicall_calls).call()
    
    # Decode results
    decoded_results = []
    for i, (contract_address, function_name, _) in enumerate(calls):
        function_abi = function_abis[function_name]
        output_type = function_abi['outputs'][0]['type'] if len(function_abi['outputs']) > 0 else None
        
        if not result[i] or result[i] == "0x":
            # Handle empty result
            logger.warning(f"Empty result for {function_name} on {contract_address}")
            decoded_results.append(None)
            continue
            
        # For simple uint256 returns (which covers most gauge functions), manually decode
        if output_type == 'uint256':
            try:
                # Decode uint256 value
                value = int.from_bytes(result[i], byteorder='big')
                decoded_results.append(value)
            except Exception as e:
                logger.error(f"Error decoding uint256 result for {function_name}: {e}")
                decoded_results.append(None)
        # For address returns
        elif output_type == 'address':
            try:
                # Address is the last 20 bytes of the result
                # Pad with zeroes if result is too short
                padded_result = result[i].ljust(40, b'\0')
                address_bytes = padded_result[-20:]
                address_hex = "0x" + address_bytes.hex()
                decoded_results.append(web3.to_checksum_address(address_hex))
            except Exception as e:
                logger.error(f"Error decoding address result for {function_name}: {e}")
                decoded_results.append(None)
        # For boolean returns
        elif output_type == 'bool':
            try:
                # Boolean is 1 or 0 at the last byte
                value = int.from_bytes(result[i], byteorder='big')
                decoded_results.append(bool(value))
            except Exception as e:
                logger.error(f"Error decoding boolean result for {function_name}: {e}")
                decoded_results.append(None)
        else:
            # For more complex types, try using the Web3.py built-in decoder
            try:
                contract = contracts[contract_address]
                if hasattr(contract, 'decode_function_result'):
                    # Web3.py >= 5.0
                    decoded = contract.decode_function_result(function_name, result[i])
                    logger.debug(f"Decoded result type: {type(decoded)} value: {decoded}")
                    if isinstance(decoded, tuple) and len(decoded) == 1:
                        decoded_results.append(decoded[0])
                    else:
                        decoded_results.append(decoded)
                else:
                    # Fallback for older versions
                    logger.warning(f"Warning: Your Web3.py version may not support automatic decoding")
                    decoded_results.append(None)
            except Exception as e:
                logger.error(f"Error decoding complex result for {function_name}: {e}")
                decoded_results.append(None)
    
    return decoded_results 
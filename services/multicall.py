"""
Multicall functionality to batch blockchain calls
"""
from typing import List, Dict, Any, Tuple
from web3 import Web3
from .abis.multicall import MULTICALL_ABI
import json

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
    multicall = web3.eth.contract(address=web3.to_checksum_address(MULTICALL_ADDRESS), abi=MULTICALL_ABI)
    
    # Prepare calls for multicall
    multicall_calls = []
    contracts = {}
    function_abis = {}
    
    # If no ABI provided, import Gauge ABI
    if abi is None:
        from .abis.gauge_abi import GAUGE_ABI
        abi = GAUGE_ABI
    
    # Cache contracts to avoid recreating them
    for contract_address, function_name, function_args in calls:
        if contract_address not in contracts:
            # Get or create contract
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
    try:
        _, result = multicall.functions.aggregate(multicall_calls).call()
    except Exception as e:
        print(f"Error calling multicall: {e}")
        raise
    
    # Decode results - simplified approach that works with most Web3.py versions
    decoded_results = []
    for i, (contract_address, function_name, _) in enumerate(calls):
        function_abi = function_abis[function_name]
        
        # For simple uint256 returns (which covers most gauge functions), manually decode
        if len(function_abi['outputs']) == 1 and function_abi['outputs'][0]['type'] == 'uint256':
            try:
                # Decode uint256 value
                value = int.from_bytes(result[i], byteorder='big')
                decoded_results.append(value)
            except Exception as e:
                print(f"Error decoding result for {function_name}: {e}")
                decoded_results.append(None)
        else:
            # For more complex types, try using the Web3.py built-in decoder
            try:
                contract = contracts[contract_address]
                if hasattr(contract, 'decode_function_result'):
                    # Web3.py >= 5.0
                    decoded = contract.decode_function_result(function_name, result[i])
                    if isinstance(decoded, tuple) and len(decoded) == 1:
                        decoded_results.append(decoded[0])
                    else:
                        decoded_results.append(decoded)
                else:
                    # Fallback for older versions - this isn't perfect but better than nothing
                    print(f"Warning: Your Web3.py version may not support automatic decoding")
                    decoded_results.append(None)
            except Exception as e:
                print(f"Error decoding complex result for {function_name}: {e}")
                decoded_results.append(None)
    
    return decoded_results 
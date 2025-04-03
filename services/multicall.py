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
    
    # Decode results
    decoded_results = []
    for i, (contract_address, function_name, _) in enumerate(calls):
        contract = contracts[contract_address]
        
        # Use the contract's decoder which is more reliable across versions
        try:
            # Get the function selector for the result we're trying to decode
            func_obj = getattr(contract.functions, function_name)
            
            # Different Web3.py versions support different decoding methods
            if hasattr(contract, 'decode_function_result'):
                # Web3.py >= 5.0
                decoded = contract.decode_function_result(function_name, result[i])
            else:
                # Web3.py < 5.0
                decoded = func_obj.call_decoder(result[i])
            
            # Output can be a single value or tuple, normalize to just return the value for single outputs
            if isinstance(decoded, tuple) and len(decoded) == 1:
                decoded_results.append(decoded[0])
            else:
                decoded_results.append(decoded)
                
        except Exception as e:
            print(f"Error decoding result for {function_name}: {e}")
            # Manual decoding as last resort for basic uint256 returns
            try:
                # Most gauge functions return uint256 which we can decode manually
                # This handles basic uint256 returns without needing eth_abi
                if len(result[i]) >= 32:  # uint256 is 32 bytes
                    int_value = int.from_bytes(result[i], byteorder='big')
                    decoded_results.append(int_value)
                else:
                    decoded_results.append(None)
            except Exception as manual_error:
                print(f"Manual decoding also failed: {manual_error}")
                decoded_results.append(None)
    
    return decoded_results 
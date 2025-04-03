"""
Script to calculate the boost value for a given address based on gauge interactions.
"""
from typing import Optional, Dict, List, Any
import json
from .constants import MAX_BOOST, PER_MAX_BOOST
from .web3_services import setup_web3, get_contract
from .multicall import batch_calls
from .abis.gauge_abi import GAUGE_ABI

class BoostService:
    def __init__(self, provider_url=None):
        """
        Initialize the BoostService with blockchain connection.
        
        Args:
            provider_url: Optional custom provider URL
        """
        self.web3 = setup_web3(provider_url)
        
    def get_boost(self, wallet_address: str, gauge_address: str) -> Optional[float]:
        """
        Calculate the boost value for a specific wallet address.
        
        Args:
            wallet_address: The wallet address to calculate boost for
            gauge_address: The address of the gauge contract
            
        Returns:
            The calculated boost value as a float, or None if calculation fails
        """
        try:
            # Make sure addresses are checksum addresses
            wallet_address = self.web3.to_checksum_address(wallet_address)
            gauge_address = self.web3.to_checksum_address(gauge_address)
            
            # Get gauge contract
            gauge = get_contract(self.web3, gauge_address, GAUGE_ABI)
            
            # Call contract functions
            working_balance = gauge.functions.working_balances(wallet_address).call()
            gauge_balance = gauge.functions.balanceOf(wallet_address).call()
            
            if gauge_balance == 0:
                return 1.0  # Default boost when no balance
                
            boost = working_balance / (PER_MAX_BOOST * gauge_balance)
            return min(max(boost, 1.0), MAX_BOOST)
            
        except Exception as e:
            print(f"Error calculating boost for address {wallet_address}: {e}")
            return None
            
    def get_boosts_batch(self, wallet_addresses: List[str], gauge_address: str) -> Dict[str, Optional[float]]:
        """
        Calculate boost values for multiple wallets in a single batch request.
        
        Args:
            wallet_addresses: List of wallet addresses to calculate boosts for
            gauge_address: The gauge contract address
            
        Returns:
            Dictionary mapping wallet addresses to their boost values
        """
        try:
            # Make sure gauge address is checksum
            gauge_address = self.web3.to_checksum_address(gauge_address)
            
            # Prepare batch calls
            calls = []
            normalized_addresses = []
            
            for wallet in wallet_addresses:
                wallet = self.web3.to_checksum_address(wallet)
                normalized_addresses.append(wallet)
                
                # Add working_balances call
                calls.append((gauge_address, "working_balances", [wallet]))
                # Add balanceOf call
                calls.append((gauge_address, "balanceOf", [wallet]))
            
            # Execute batch call
            results = batch_calls(self.web3, calls)
            
            # Process results
            boosts = {}
            for i in range(0, len(normalized_addresses)):
                wallet = normalized_addresses[i]
                working_balance = results[i*2]  # Even indices have working_balances
                gauge_balance = results[i*2+1]  # Odd indices have balanceOf
                
                # Safety check for None values
                if working_balance is None or gauge_balance is None:
                    boosts[wallet] = None
                    continue
                    
                if gauge_balance == 0:
                    boosts[wallet] = 1.0
                else:
                    boost = working_balance / (PER_MAX_BOOST * gauge_balance)
                    boosts[wallet] = min(max(boost, 1.0), MAX_BOOST)
                    
            return boosts
            
        except Exception as e:
            print(f"Error calculating batch boosts: {e}")
            return {wallet: None for wallet in wallet_addresses}


# Example usage
if __name__ == "__main__":
    from .constants import PROVIDER_WALLETS
    
    boost_service = BoostService()
    
    # Example gauge contract
    gauge_address = "0x09F62a6777032329C0d49F1FD4fBe9b3468CDa56"
    
    # Individual calls
    print("Individual calls:")
    for provider, wallet in PROVIDER_WALLETS.items():
        boost = boost_service.get_boost(wallet, gauge_address)
        if boost is not None:
            print(f"{provider.capitalize()} ({wallet}) has a boost of {boost:.4f}")
        else:
            print(f"Failed to calculate boost for {provider}")
    
    # Batch call
    print("\nBatch call:")
    wallet_addresses = list(PROVIDER_WALLETS.values())
    batch_results = boost_service.get_boosts_batch(wallet_addresses, gauge_address)
    
    for provider, wallet in PROVIDER_WALLETS.items():
        boost = batch_results.get(wallet)
        if boost is not None:
            print(f"{provider.capitalize()} ({wallet}) has a boost of {boost:.4f}")
        else:
            print(f"Failed to calculate boost for {provider}")

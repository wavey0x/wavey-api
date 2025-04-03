"""
API service to get information about a specific Curve gauge
"""
import requests
from typing import Dict, Any, Optional, List
import json
from .verify_gauge import verify_gauge_by_address
from .constants import PROVIDER_WALLETS, MAX_BOOST, PER_MAX_BOOST
from .web3_services import setup_web3, get_contract
from .boost import BoostService
from .abis.gauge_abi import GAUGE_ABI

class GaugeInfoService:
    def __init__(self, curve_api_url: str = "https://api.curve.fi/api/getAllGauges"):
        """
        Initialize the GaugeInfoService
        
        Args:
            curve_api_url: URL of the Curve API to fetch gauge data
        """
        self.curve_api_url = curve_api_url
        self.web3 = setup_web3()
        self.boost_service = BoostService()
        self._gauge_data_cache = None
        self._last_update = 0
    
    def _fetch_all_gauges(self) -> Dict[str, Any]:
        """
        Fetch data about all gauges from the Curve API
        
        Returns:
            Dictionary containing all gauge data
        """
        try:
            response = requests.get(self.curve_api_url)
            response.raise_for_status()
            data = response.json()
            if data.get("success"):
                return data.get("data", {})
            return {}
        except Exception as e:
            print(f"Error fetching gauge data: {e}")
            return {}
    
    def _find_gauge_by_address(self, gauge_address: str) -> Optional[Dict[str, Any]]:
        """
        Find gauge information by gauge address
        
        Args:
            gauge_address: The gauge address to look for
            
        Returns:
            Dictionary with gauge information or None if not found
        """
        all_gauges = self._fetch_all_gauges()
        
        # Normalize the gauge address for comparison
        gauge_address = gauge_address.lower()
        
        # Look through all pools for matching gauge address
        for pool_name, pool_data in all_gauges.items():
            if "gauge" in pool_data:
                pool_gauge_address = pool_data["gauge"].lower()
                if pool_gauge_address == gauge_address:
                    return {
                        "pool_name": pool_name,
                        "pool_data": pool_data
                    }
        
        return None
    
    def get_provider_boosts(self, gauge_address: str) -> Dict[str, Any]:
        """
        Get boost values for all providers for a specific gauge using batch requests
        
        Args:
            gauge_address: The gauge address to calculate boosts for
            
        Returns:
            Dictionary containing boost values for each provider
        """
        provider_boosts = {}
        
        # Get all wallet addresses
        wallet_addresses = list(PROVIDER_WALLETS.values())
        
        # Use the batch function to get all boosts at once
        batch_results = self.boost_service.get_boosts_batch(wallet_addresses, gauge_address)
        
        # Format the results
        for provider_name, wallet_address in PROVIDER_WALLETS.items():
            boost = batch_results.get(wallet_address)
            provider_boosts[provider_name] = {
                "wallet": wallet_address,
                "boost": boost,
                "boost_formatted": f"{boost:.4f}" if boost is not None else "N/A"
            }
        
        return provider_boosts
    
    def get_gauge_info(self, request) -> Dict[str, Any]:
        """
        Get information about a specific gauge
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with gauge information and verification status
        """
        response = {
            "success": False,
            "message": "",
            "data": None
        }
        
        # Get gauge address from request
        gauge_address = request.args.get('gauge')
        if not gauge_address:
            response["message"] = "Missing 'gauge' parameter"
            return response
        
        # Get verification status
        verification = verify_gauge_by_address(gauge_address)
        
        # Find gauge information
        gauge_info = self._find_gauge_by_address(gauge_address)
        
        if not gauge_info:
            response["message"] = "Gauge not found in Curve API"
            response["verification"] = verification
            return response
        
        # Get provider boosts
        provider_boosts = self.get_provider_boosts(gauge_address)
        
        # Extract relevant information
        pool_data = gauge_info["pool_data"]
        pool_name = gauge_info["pool_name"]
        
        # Prepare response
        response["success"] = True
        response["message"] = "Gauge information retrieved successfully"
        response["data"] = {
            "pool_name": pool_name,
            "gauge_address": gauge_address,
            "pool_address": pool_data.get("poolAddress") or pool_data.get("swap"),
            "lp_token": pool_data.get("swap_token"),
            "blockchain": pool_data.get("blockchainId", "ethereum"),
            "side_chain": pool_data.get("side_chain", False),
            "gauge_data": {
                "inflation_rate": pool_data.get("gauge_data", {}).get("inflation_rate"),
                "working_supply": pool_data.get("gauge_data", {}).get("working_supply")
            },
            "gauge_controller": pool_data.get("gauge_controller", {}),
            "gauge_relative_weight": pool_data.get("gauge_controller", {}).get("gauge_relative_weight"),
            "is_killed": pool_data.get("is_killed", False),
            "has_no_crv": pool_data.get("hasNoCrv", False),
            "pool_type": pool_data.get("type"),
            "factory": pool_data.get("factory", False),
            "provider_boosts": provider_boosts
        }
        response["verification"] = verification
        
        return response


# Example of how to use in a Flask or similar framework
"""
from flask import Flask, request, jsonify
from services.gauge_info import GaugeInfoService

app = Flask(__name__)
gauge_service = GaugeInfoService()

@app.route('/api/gauge', methods=['GET'])
def get_gauge_info():
    response = gauge_service.get_gauge_info(request)
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
""" 
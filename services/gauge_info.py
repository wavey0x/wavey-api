"""
API service to get information about a specific Curve gauge
"""
import requests
import time
import logging
from typing import Dict, Any, Optional, List
import json
from datetime import datetime, timedelta
from .verify_gauge import verify_gauge_by_address
from .constants import PROVIDER_WALLETS, MAX_BOOST, PER_MAX_BOOST
from .web3_services import setup_web3, get_contract
from .boost import BoostService
from .abis.gauge_abi import GAUGE_ABI
from collections import defaultdict

# Configure logger
logger = logging.getLogger(__name__)

# Create a session for connection pooling
http_session = requests.Session()

# Cache expiration time in seconds
CACHE_EXPIRATION_SECONDS = 100

class GaugeInfoService:
    def __init__(self, curve_api_url: str = "https://api.curve.finance/api/getAllGauges"):
        """
        Initialize the GaugeInfoService with caching capabilities
        
        Args:
            curve_api_url: URL of the Curve API to fetch gauge data
        """
        self.curve_api_url = curve_api_url
        self.web3 = setup_web3()
        self.boost_service = BoostService()
        
        # Initialize cache
        self._gauge_data_cache = None
        self._cache_timestamp = 0
        self._cache_hits = 0
        self._cache_misses = 0
    
    def _is_cache_valid(self) -> bool:
        """
        Check if the cache is still valid
        
        Returns:
            True if cache is valid, False otherwise
        """
        current_time = time.time()
        cache_age = current_time - self._cache_timestamp
        
        return (self._gauge_data_cache is not None and 
                cache_age < CACHE_EXPIRATION_SECONDS)
    
    def _fetch_all_gauges(self) -> Dict[str, Any]:
        """
        Fetch data about all gauges from the Curve API with caching
        
        Returns:
            Dictionary containing all gauge data
        """
        # Check if cache is valid
        if self._is_cache_valid():
            self._cache_hits += 1
            cache_age = time.time() - self._cache_timestamp
            logger.info(f"Using cached gauge data (age: {cache_age:.1f}s, hits: {self._cache_hits}, misses: {self._cache_misses})")
            return self._gauge_data_cache
        
        # Cache is invalid, need to fetch fresh data
        self._cache_misses += 1
        start_time = time.time()
        try:
            logger.info(f"Fetching fresh gauge data from Curve API (cache expired or not initialized)")
            response = http_session.get(self.curve_api_url, timeout=10)  # Use session for connection pooling
            response.raise_for_status()
            data = response.json()
            
            if data.get("success"):
                # Update cache
                self._gauge_data_cache = data.get("data", {})
                self._cache_timestamp = time.time()
                
                elapsed = time.time() - start_time
                gauge_count = len(self._gauge_data_cache)
                logger.info(f"Updated cache with {gauge_count} gauges in {elapsed:.3f}s (hits: {self._cache_hits}, misses: {self._cache_misses})")
                return self._gauge_data_cache
            
            logger.warning(f"Curve API returned success=false in {time.time() - start_time:.3f}s")
            # Use stale cache if available
            if self._gauge_data_cache is not None:
                logger.warning(f"Using stale cache due to API error (success=false)")
                return self._gauge_data_cache
            return {}
        except requests.exceptions.Timeout:
            elapsed = time.time() - start_time
            logger.error(f"Timeout fetching gauge data after {elapsed:.3f}s")
            if self._gauge_data_cache is not None:
                logger.warning(f"Using stale cache due to API timeout")
                return self._gauge_data_cache
            return {}
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Error fetching gauge data in {elapsed:.3f}s: {e}")
            
            # Return cached data even if expired in case of API error
            if self._gauge_data_cache is not None:
                logger.warning(f"Using stale cache due to API error: {type(e).__name__}")
                return self._gauge_data_cache
            
            return {}
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the cache
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        cache_age = current_time - self._cache_timestamp
        next_refresh = max(0, CACHE_EXPIRATION_SECONDS - cache_age)
        
        return {
            "has_cached_data": self._gauge_data_cache is not None,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_age_seconds": cache_age if self._gauge_data_cache is not None else None,
            "next_refresh_seconds": next_refresh if self._gauge_data_cache is not None else 0,
            "timestamp": datetime.fromtimestamp(self._cache_timestamp).isoformat() if self._cache_timestamp > 0 else None,
            "next_refresh_time": datetime.fromtimestamp(self._cache_timestamp + CACHE_EXPIRATION_SECONDS).isoformat() if self._cache_timestamp > 0 else None,
            "gauge_count": len(self._gauge_data_cache) if self._gauge_data_cache is not None else 0
        }
    
    def force_refresh_cache(self) -> Dict[str, Any]:
        """
        Force refresh the gauge data cache
        
        Returns:
            Dictionary with refresh status and statistics
        """
        # Reset cache
        self._gauge_data_cache = None
        self._cache_timestamp = 0
        
        # Fetch new data
        start_time = time.time()
        gauge_data = self._fetch_all_gauges()
        elapsed = time.time() - start_time
        
        return {
            "success": len(gauge_data) > 0,
            "elapsed_seconds": elapsed,
            "gauge_count": len(gauge_data),
            "cache_stats": self.get_cache_stats()
        }
    
    def _find_gauge_by_address(self, gauge_address: str) -> Optional[Dict[str, Any]]:
        """
        Find gauge information by gauge address
        
        Args:
            gauge_address: The gauge address to look for
            
        Returns:
            Dictionary with gauge information or None if not found
        """
        start_time = time.time()
        all_gauges = self._fetch_all_gauges()
        
        # Normalize the gauge address for comparison
        gauge_address = gauge_address.lower()
        
        # Look through all pools for matching gauge address
        for pool_name, pool_data in all_gauges.items():
            if "gauge" in pool_data:
                pool_gauge_address = pool_data["gauge"].lower()
                if pool_gauge_address == gauge_address:
                    elapsed = time.time() - start_time
                    logger.info(f"Found gauge {gauge_address} for pool {pool_name} in {elapsed:.3f}s")
                    return {
                        "pool_name": pool_name,
                        "pool_data": pool_data
                    }
        
        elapsed = time.time() - start_time
        logger.warning(f"Gauge {gauge_address} not found in {elapsed:.3f}s")
        return None
    
    def get_provider_boosts(self, gauge_address: str) -> Dict[str, Any]:
        """
        Get boost values and supply percentages for all providers for a specific gauge
        
        Args:
            gauge_address: The gauge address to calculate boosts for
            
        Returns:
            Dictionary containing boost values and supply percentages for each provider
        """
        start_time = time.time()
        provider_boosts = {}
        
        # Get all wallet addresses
        wallet_addresses = list(PROVIDER_WALLETS.values())
        
        # Use the batch function to get all boosts at once
        logger.info(f"Fetching boosts for {len(wallet_addresses)} providers for gauge {gauge_address}")
        batch_results = self.boost_service.get_boosts_batch(wallet_addresses, gauge_address)
        
        # Format the results
        for provider_name, wallet_address in PROVIDER_WALLETS.items():
            provider_data = batch_results.get(wallet_address, {})
            boost = provider_data.get("boost")
            pct_of_total = provider_data.get("pct_of_total_supply", 0)
            gauge_balance = provider_data.get("gauge_balance", 0)
            
            provider_boosts[provider_name] = {
                "wallet": wallet_address,
                "boost": boost,
                "boost_formatted": f"{boost:.4f}" if boost is not None else "N/A",
                "pct_of_total_supply": pct_of_total,
                "pct_formatted": f"{pct_of_total:.2f}%" if pct_of_total is not None else "0.00%",
                "gauge_balance": gauge_balance
            }
        
        elapsed = time.time() - start_time
        logger.info(f"Calculated boosts for {len(wallet_addresses)} providers in {elapsed:.3f}s")
        return provider_boosts
    
    def get_gauge_info(self, request) -> Dict[str, Any]:
        """
        Get information about a specific gauge
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with gauge information and verification status
        """
        request_start_time = time.time()
        logger.info(f"Starting gauge info request with params: {request.args}")
        
        response = {
            "success": False,
            "message": "",
            "data": None
        }
        
        # Get gauge address from request
        gauge_address = request.args.get('gauge')
        if not gauge_address:
            response["message"] = "Missing 'gauge' parameter"
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Missing gauge parameter. Took {elapsed:.3f}s")
            return response
        
        # Get verification status
        verification_start = time.time()
        verification = verify_gauge_by_address(gauge_address)
        verification_time = time.time() - verification_start
        logger.info(f"Gauge verification took {verification_time:.3f}s. Is valid: {verification['is_valid']}")
        
        # Find gauge information
        find_gauge_start = time.time()
        gauge_info = self._find_gauge_by_address(gauge_address)
        find_gauge_time = time.time() - find_gauge_start
        
        if not gauge_info:
            response["message"] = "Gauge not found in Curve API"
            response["verification"] = verification
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Gauge {gauge_address} not found in Curve API. Took {elapsed:.3f}s")
            return response
        
        # Get provider boosts
        boost_start = time.time()
        provider_boosts = self.get_provider_boosts(gauge_address)
        boost_time = time.time() - boost_start
        logger.info(f"Provider boost calculation took {boost_time:.3f}s")
        
        # Extract relevant information
        pool_data = gauge_info["pool_data"]
        pool_name = gauge_info["pool_name"]
        
        # Extract APY information - provides min and max boost APY values
        gauge_crv_apy = pool_data.get("gaugeCrvApy", [None, None])
        gauge_future_crv_apy = pool_data.get("gaugeFutureCrvApy", [None, None])
        
        # Extract pool URLs for user actions - only take the first URL from each array
        pool_urls_raw = pool_data.get("poolUrls", {})
        pool_urls = {
            "swap": pool_urls_raw.get("swap", [])[0] if pool_urls_raw.get("swap") else None,
            "deposit": pool_urls_raw.get("deposit", [])[0] if pool_urls_raw.get("deposit") else None,
            "withdraw": pool_urls_raw.get("withdraw", [])[0] if pool_urls_raw.get("withdraw") else None
        }
        
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
            # Add APY information with clear labeling
            "apy_data": {
                "gauge_crv_apy": {
                    "min_boost": gauge_crv_apy[0],
                    "max_boost": gauge_crv_apy[1],
                    "raw_values": gauge_crv_apy
                },
                "gauge_future_crv_apy": {
                    "min_boost": gauge_future_crv_apy[0],
                    "max_boost": gauge_future_crv_apy[1],
                    "raw_values": gauge_future_crv_apy
                }
            },
            # Add pool URLs for direct links to Curve UI - only first URL
            "pool_urls": pool_urls,
            "gauge_controller": pool_data.get("gauge_controller", {}),
            "gauge_relative_weight": pool_data.get("gauge_controller", {}).get("gauge_relative_weight"),
            "is_killed": pool_data.get("is_killed", False),
            "has_no_crv": pool_data.get("hasNoCrv", False),
            "pool_type": pool_data.get("type"),
            "factory": pool_data.get("factory", False),
            "provider_boosts": provider_boosts
        }
        response["verification"] = verification
        
        # Add timing information
        total_elapsed = time.time() - request_start_time
        response["timing"] = {
            "total_seconds": total_elapsed,
            "verification_seconds": verification_time,
            "find_gauge_seconds": find_gauge_time,
            "boost_calculation_seconds": boost_time
        }
        
        logger.info(f"Gauge info request completed in {total_elapsed:.3f}s for gauge {gauge_address}")
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
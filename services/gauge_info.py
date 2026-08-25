"""
API service to get information about a specific Curve gauge
"""
import requests
import time
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from .verify_gauge import verify_gauge_by_address
from .constants import PROVIDER_WALLETS, MAX_BOOST, PER_MAX_BOOST
from .web3_services import setup_web3, get_contract
from .boost import BoostService
from .abis.gauge_abi import GAUGE_ABI
from collections import defaultdict
from .crvlol_snapshot import get_snapshot_path, load_snapshot

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
            curve_api_url: URL of the Curve API to fetch gauge data (fallback)
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
        
        is_valid = (self._gauge_data_cache is not None and 
                   cache_age < CACHE_EXPIRATION_SECONDS)
        
        logger.debug(f"Cache validation: cache_data={self._gauge_data_cache is not None}, age={cache_age:.1f}s, ttl={CACHE_EXPIRATION_SECONDS}s, is_valid={is_valid}")
        
        return is_valid
    
    def _get_local_curve_data(self) -> Optional[Dict[str, Any]]:
        """
        Get curve gauge data from local file to reduce latency
        
        Returns:
            Dictionary containing curve gauge data or None if file not found
        """
        try:
            gauge_data = load_snapshot().get("curve_gauge_data")
            if not isinstance(gauge_data, dict):
                logger.warning("CRV snapshot has no curve_gauge_data object")
                return None
            logger.info("Loaded %s gauges from CRV snapshot", len(gauge_data))
            return gauge_data
        except Exception as e:
            logger.warning("Unable to load CRV snapshot: %s", e)
            return None
    
    def _fetch_all_gauges(self) -> Dict[str, Any]:
        """
        Fetch data about all gauges from local file first, fallback to Curve API
        
        Returns:
            Dictionary containing all gauge data
        """
        # Check if cache is valid
        if self._is_cache_valid():
            self._cache_hits += 1
            cache_age = time.time() - self._cache_timestamp
            logger.info(f"Using cached gauge data (age: {cache_age:.1f}s, hits: {self._cache_hits}, misses: {self._cache_misses})")
            return self._gauge_data_cache
        
        # Cache is invalid, try local file first
        self._cache_misses += 1
        start_time = time.time()
        
        # Try to get data from local file first
        logger.info("Cache invalid, attempting to load from local file...")
        local_data = self._get_local_curve_data()
        
        if local_data:
            # Update cache with local data
            self._gauge_data_cache = local_data
            self._cache_timestamp = time.time()
            
            elapsed = time.time() - start_time
            gauge_count = len(self._gauge_data_cache)
            logger.info(f"Successfully updated cache with {gauge_count} gauges from local file in {elapsed:.3f}s (hits: {self._cache_hits}, misses: {self._cache_misses})")
            
            # Debug: Log some sample data to verify structure
            if isinstance(local_data, dict) and len(local_data) > 0:
                sample_keys = list(local_data.keys())[:3]
                logger.info(f"Sample gauge keys from local file: {sample_keys}")
                if sample_keys:
                    sample_data = local_data[sample_keys[0]]
                    logger.debug(f"Sample gauge data structure: {type(sample_data)} - keys: {list(sample_data.keys()) if isinstance(sample_data, dict) else 'N/A'}")
            
            return self._gauge_data_cache
        else:
            logger.warning("Failed to load data from local file")
        
        # Fallback to external API if local file not available
        logger.warning("Local curve gauge data not available, falling back to external API")
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
                logger.info(f"Updated cache with {gauge_count} gauges from external API in {elapsed:.3f}s (hits: {self._cache_hits}, misses: {self._cache_misses})")
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
        
        # Determine data source
        data_source = "none"
        if self._gauge_data_cache is not None:
            try:
                data_source = "local_file" if get_snapshot_path().is_file() else "external_api"
            except RuntimeError:
                data_source = "unknown"
        
        return {
            "has_cached_data": self._gauge_data_cache is not None,
            "data_source": data_source,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_age_seconds": cache_age if self._cache_timestamp > 0 else None,
            "next_refresh_seconds": next_refresh if self._cache_timestamp > 0 else 0,
            "timestamp": datetime.fromtimestamp(self._cache_timestamp).isoformat() if self._cache_timestamp > 0 else None,
            "next_refresh_time": datetime.fromtimestamp(self._cache_timestamp + CACHE_EXPIRATION_SECONDS).isoformat() if self._cache_timestamp > 0 else None,
            "gauge_count": len(self._gauge_data_cache) if self._gauge_data_cache is not None else 0
        }
    
    def force_refresh_cache(self) -> Dict[str, Any]:
        """
        Force refresh the gauge data cache from local file first, then external API
        
        Returns:
            Dictionary with refresh status and statistics
        """
        # Reset cache
        self._gauge_data_cache = None
        self._cache_timestamp = 0
        
        # Try local file first
        start_time = time.time()
        local_data = self._get_local_curve_data()
        
        if local_data:
            # Update cache with local data
            self._gauge_data_cache = local_data
            self._cache_timestamp = time.time()
            elapsed = time.time() - start_time
            
            return {
                "success": True,
                "source": "local_file",
                "elapsed_seconds": elapsed,
                "gauge_count": len(local_data),
                "cache_stats": self.get_cache_stats()
            }
        
        # Fallback to external API if local file not available
        logger.warning("Local curve gauge data not available, falling back to external API")
        gauge_data = self._fetch_all_gauges()
        elapsed = time.time() - start_time
        
        return {
            "success": len(gauge_data) > 0,
            "source": "external_api",
            "elapsed_seconds": elapsed,
            "gauge_count": len(gauge_data),
            "cache_stats": self.get_cache_stats()
        }
    
    def force_refresh_from_local(self) -> Dict[str, Any]:
        """
        Force refresh the gauge data cache specifically from local file
        
        Returns:
            Dictionary with refresh status and statistics
        """
        start_time = time.time()
        
        # Reset cache
        self._gauge_data_cache = None
        self._cache_timestamp = 0
        
        # Get data from local file
        local_data = self._get_local_curve_data()
        
        if local_data:
            # Update cache with local data
            self._gauge_data_cache = local_data
            self._cache_timestamp = time.time()
            elapsed = time.time() - start_time
            
            return {
                "success": True,
                "source": "local_file",
                "elapsed_seconds": elapsed,
                "gauge_count": len(local_data),
                "cache_stats": self.get_cache_stats()
            }
        else:
            elapsed = time.time() - start_time
            return {
                "success": False,
                "source": "local_file",
                "elapsed_seconds": elapsed,
                "gauge_count": 0,
                "error": "Local curve gauge data file not found or could not be loaded",
                "cache_stats": self.get_cache_stats()
            }
    
    def force_clear_cache(self) -> Dict[str, Any]:
        """
        Force clear the cache and return current status
        
        Returns:
            Dictionary with cache status after clearing
        """
        # Clear cache
        self._gauge_data_cache = None
        self._cache_timestamp = 0
        
        logger.info("Cache forcefully cleared")
        
        return {
            "success": True,
            "message": "Cache cleared successfully",
            "cache_stats": self.get_cache_stats()
        }
    
    def get_current_data_status(self) -> Dict[str, Any]:
        """
        Get current status of the gauge data without triggering a refresh
        
        Returns:
            Dictionary with current data status
        """
        return {
            "cache_status": {
                "has_cached_data": self._gauge_data_cache is not None,
                "cache_timestamp": self._cache_timestamp,
                "cache_age_seconds": time.time() - self._cache_timestamp if self._cache_timestamp > 0 else None,
                "is_cache_valid": self._is_cache_valid()
            },
            "local_file_status": self.check_local_file_status(),
            "cache_stats": self.get_cache_stats()
        }
    
    def check_local_file_status(self) -> Dict[str, Any]:
        """
        Check the status of the local curve gauge data file
        
        Returns:
            Dictionary with file status information
        """
        try:
            filepath = get_snapshot_path()
            if not filepath.is_file():
                return {
                    "status": "error",
                    "message": "CRV snapshot not found",
                    "filepath": str(filepath)
                }
            file_stats = filepath.stat()
            file_size = file_stats.st_size
            data = load_snapshot()
            gauge_data = data.get("curve_gauge_data", {})
            return {
                "status": "success",
                "message": "CRV snapshot loaded successfully",
                "filepath": str(filepath),
                "file_size_bytes": file_size,
                "file_size_mb": round(file_size / (1024 * 1024), 2),
                "last_modified": datetime.fromtimestamp(file_stats.st_mtime).isoformat(),
                "data_type": type(data).__name__,
                "data_structure": "CRV snapshot",
                "gauge_count": len(gauge_data) if isinstance(gauge_data, dict) else 0
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error checking file: {str(e)}",
                "filepath": str(filepath) if 'filepath' in locals() else None
            }
    
    def test_local_file_loading(self) -> Dict[str, Any]:
        """
        Test local file loading directly and provide detailed diagnostics
        
        Returns:
            Dictionary with detailed test results
        """
        status = self.check_local_file_status()
        return {
            "success": status["status"] == "success",
            "file_path": status.get("filepath"),
            "gauge_count": status.get("gauge_count", 0),
            "error": None if status["status"] == "success" else status["message"]
        }
    
    def _find_gauge_by_address(self, gauge_address: str) -> Optional[Dict[str, Any]]:
        """
        Find gauge information by gauge address using direct dictionary lookup
        
        Args:
            gauge_address: The gauge address to look for
            
        Returns:
            Dictionary with gauge information or None if not found
        """
        start_time = time.time()
        all_gauges = self._fetch_all_gauges()
        
        # Debug: Log the data structure
        logger.debug(f"All gauges data type: {type(all_gauges)}")
        logger.debug(f"All gauges keys count: {len(all_gauges) if isinstance(all_gauges, dict) else 'N/A'}")
        if isinstance(all_gauges, dict) and len(all_gauges) > 0:
            sample_keys = list(all_gauges.keys())[:3]
            logger.debug(f"Sample gauge keys: {sample_keys}")
        
        # Normalize the gauge address for comparison
        gauge_address = gauge_address.lower()
        logger.debug(f"Looking for gauge address: {gauge_address}")
        
        # Direct dictionary lookup since gauge address is now the key
        if gauge_address in all_gauges:
            pool_data = all_gauges[gauge_address]
            elapsed = time.time() - start_time
            logger.info(f"Found gauge {gauge_address} in {elapsed:.3f}s using direct lookup")
            return {
                "pool_name": pool_data.get("pool_name", gauge_address),  # Use pool_name if available, otherwise gauge address
                "pool_data": pool_data
            }
        
        elapsed = time.time() - start_time
        logger.warning(f"Gauge {gauge_address} not found in {elapsed:.3f}s")
        
        # Debug: Check if the gauge address exists in a different format
        if isinstance(all_gauges, dict):
            # Check for case-insensitive match
            for key in all_gauges.keys():
                if key.lower() == gauge_address:
                    logger.info(f"Found gauge {gauge_address} with case-insensitive match to key: {key}")
                    pool_data = all_gauges[key]
                    return {
                        "pool_name": pool_data.get("pool_name", key),
                        "pool_data": pool_data
                    }
        
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
    
    def get_basic_gauge_info(self, request) -> Dict[str, Any]:
        """
        Get basic gauge information from local data only (fastest)
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with basic gauge information
        """
        request_start_time = time.time()
        logger.info(f"Starting basic gauge info request with params: {request.args}")
        
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
        
        # Find gauge information from local data only
        find_gauge_start = time.time()
        gauge_info = self._find_gauge_by_address(gauge_address)
        find_gauge_time = time.time() - find_gauge_start
        
        if not gauge_info:
            response["message"] = "Gauge not found in local data"
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Gauge {gauge_address} not found in local data. Took {elapsed:.3f}s")
            return response
        
        # Extract relevant information
        pool_data = gauge_info["pool_data"]
        pool_name = gauge_info["pool_name"]
        lendingVaultAddress = pool_data.get("lendingVaultAddress", None)
        lendingVaultUrls = pool_data.get("lendingVaultUrls", {})
        if lendingVaultUrls:
            lendingVaultUrls = {
                "deposit": lendingVaultUrls.get("deposit"),  # Already a string, no [0] needed
                "withdraw": lendingVaultUrls.get("withdraw"),  # Already a string, no [0] needed
                "borrow": lendingVaultUrls.get("borrow")  # Already a string, no [0] needed
            }
        
        # Extract APY information
        gauge_crv_apy = pool_data.get("gaugeCrvApy", [None, None])
        gauge_future_crv_apy = pool_data.get("gaugeFutureCrvApy", [None, None])
        
        # Extract pool URLs
        pool_urls_raw = pool_data.get("poolUrls", {})
        pool_urls = {
            "swap": pool_urls_raw.get("swap", [])[0] if pool_urls_raw.get("swap") else None,
            "deposit": pool_urls_raw.get("deposit", [])[0] if pool_urls_raw.get("deposit") else None,
            "withdraw": pool_urls_raw.get("withdraw", [])[0] if pool_urls_raw.get("withdraw") else None
        }
        
        # Prepare response with basic data only
        response["success"] = True
        response["message"] = "Basic gauge information retrieved successfully"
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
            "pool_urls": pool_urls,
            "gauge_controller": pool_data.get("gauge_controller", {}),
            "gauge_relative_weight": pool_data.get("gauge_controller", {}).get("gauge_relative_weight"),
            "is_killed": pool_data.get("is_killed", False),
            "has_no_crv": pool_data.get("hasNoCrv", False),
            "pool_type": pool_data.get("type"),
            "factory": pool_data.get("factory", False),
            "curve_key": pool_data.get("curve_key", None),
            "lendingVaultAddress": lendingVaultAddress,
            "lendingVaultUrls": lendingVaultUrls
        }
        
        # Add timing information
        total_elapsed = time.time() - request_start_time
        response["timing"] = {
            "total_seconds": total_elapsed,
            "find_gauge_seconds": find_gauge_time
        }
        
        logger.info(f"Basic gauge info request completed in {total_elapsed:.3f}s for gauge {gauge_address}")
        return response
    
    def get_gauge_verification(self, request) -> Dict[str, Any]:
        """
        Get gauge verification status (blockchain calls required)
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with gauge verification status
        """
        request_start_time = time.time()
        logger.info(f"Starting gauge verification request with params: {request.args}")
        
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
        
        response["success"] = True
        response["message"] = "Gauge verification completed"
        response["data"] = {
            "gauge_address": gauge_address,
            "verification": verification
        }
        
        # Add timing information
        total_elapsed = time.time() - request_start_time
        response["timing"] = {
            "total_seconds": total_elapsed,
            "verification_seconds": verification_time
        }
        
        logger.info(f"Gauge verification completed in {total_elapsed:.3f}s for gauge {gauge_address}")
        return response
    
    def get_gauge_boosts(self, request) -> Dict[str, Any]:
        """
        Get provider boost calculations for a gauge (blockchain calls required)
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with provider boost information
        """
        request_start_time = time.time()
        logger.info(f"Starting gauge boost calculation request with params: {request.args}")
        
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
        
        # Get provider boosts
        boost_start = time.time()
        provider_boosts = self.get_provider_boosts(gauge_address)
        boost_time = time.time() - boost_start
        
        response["success"] = True
        response["message"] = "Provider boost calculations completed"
        response["data"] = {
            "gauge_address": gauge_address,
            "provider_boosts": provider_boosts
        }
        
        # Add timing information
        total_elapsed = time.time() - request_start_time
        response["timing"] = {
            "total_seconds": total_elapsed,
            "boost_calculation_seconds": boost_time
        }
        
        logger.info(f"Gauge boost calculation completed in {total_elapsed:.3f}s for gauge {gauge_address}")
        return response
    
    def get_complete_gauge_info(self, request) -> Dict[str, Any]:
        """
        Get complete gauge information including all data (original method for backward compatibility)
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with complete gauge information
        """
        request_start_time = time.time()
        logger.info(f"Starting complete gauge info request with params: {request.args}")
        
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
            response["message"] = "Gauge not found in local data"
            response["verification"] = verification
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Gauge {gauge_address} not found in local data. Took {elapsed:.3f}s")
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

        lendingVaultAddress = pool_data.get("lendingVaultAddress", None)
        lendingVaultUrls = pool_data.get("lendingVaultUrls", {})
        if lendingVaultUrls:
            lendingVaultUrls = {
                "deposit": lendingVaultUrls.get("deposit"),  # Already a string
                "withdraw": lendingVaultUrls.get("withdraw"),  # Already a string
                "borrow": lendingVaultUrls.get("borrow")  # Already a string
            }
        
        # Prepare response
        response["success"] = True
        response["message"] = "Complete gauge information retrieved successfully"
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
            "lendingVaultAddress": lendingVaultAddress,
            "lendingVaultUrls": lendingVaultUrls,
            "gauge_controller": pool_data.get("gauge_controller", {}),
            "gauge_relative_weight": pool_data.get("gauge_controller", {}).get("gauge_relative_weight"),
            "is_killed": pool_data.get("is_killed", False),
            "has_no_crv": pool_data.get("hasNoCrv", False),
            "pool_type": pool_data.get("type"),
            "factory": pool_data.get("factory", False),
            "provider_boosts": provider_boosts,
            "curve_key": pool_data.get("curve_key", None)
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
        
        logger.info(f"Complete gauge info request completed in {total_elapsed:.3f}s for gauge {gauge_address}")
        return response
    
    def get_gauge_info(self, request) -> Dict[str, Any]:
        """
        Get information about a specific gauge (backward compatibility - calls get_complete_gauge_info)
        
        Args:
            request: HTTP request object containing the gauge parameter
            
        Returns:
            Dictionary with complete gauge information
        """
        return self.get_complete_gauge_info(request)
    
    def search_gauges_by_name(self, request) -> Dict[str, Any]:
        """
        Search gauges by name using fuzzy search (case insensitive)
        
        Args:
            request: HTTP request object containing the 'q' (query) parameter
            
        Returns:
            Dictionary with search results sorted by inflation rate
        """
        request_start_time = time.time()
        logger.info(f"Starting gauge search request with params: {request.args}")
        
        response = {
            "success": False,
            "message": "",
            "data": None
        }
        
        # Get search query from request
        query = request.args.get('q', '').strip()
        if not query:
            response["message"] = "Missing 'q' (query) parameter"
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Missing query parameter. Took {elapsed:.3f}s")
            return response
        
        # Require minimum 3 characters
        if len(query) < 3:
            response["message"] = "Query must be at least 3 characters long"
            elapsed = time.time() - request_start_time
            logger.warning(f"Request failed: Query too short ({len(query)} chars). Took {elapsed:.3f}s")
            return response
        
        try:
            data = load_snapshot()
            # Get the curve_gauges_by_name data
            gauges_by_name = data.get("curve_gauges_by_name", {})
            if not gauges_by_name:
                response["message"] = "No gauge data available"
                return response
            
            # Debug: Log the structure of the first few items
            logger.debug(f"curve_gauges_by_name type: {type(gauges_by_name)}")
            if isinstance(gauges_by_name, dict) and len(gauges_by_name) > 0:
                sample_keys = list(gauges_by_name.keys())[:3]
                logger.debug(f"Sample keys: {sample_keys}")
                for key in sample_keys:
                    sample_value = gauges_by_name[key]
                    logger.debug(f"Sample value for '{key}': {type(sample_value)} - {sample_value}")
            
            # Perform fuzzy search (case insensitive)
            query_lower = query.lower()
            search_results = []
            
            for gauge_name, gauge_data in gauges_by_name.items():
                # Check if query is contained in the gauge name (case insensitive)
                if query_lower in gauge_name.lower():
                    # Handle the data structure - gauge_data is always a dict with gauge and inflation_rate
                    if isinstance(gauge_data, dict):
                        # Extract fields from the structure
                        inflation_rate = gauge_data.get("inflation_rate", 0)
                        
                        # Inflation rate is now already an integer, no conversion needed
                        result = {
                            "name": gauge_name,
                            "gauge_address": gauge_data.get("gauge_address"),  # Get from "gauge" field
                            "inflation_rate": inflation_rate  # Already an integer
                        }
                        search_results.append(result)
                    else:
                        # Skip unexpected data types
                        logger.warning(f"Unexpected data type for gauge '{gauge_name}': {type(gauge_data)}")
                        continue
            
            # Sort results by inflation_rate (descending - highest first)
            # Inflation rates are now already integers, so sorting will work correctly
            search_results.sort(key=lambda x: x.get("inflation_rate", 0), reverse=True)
            
            # Limit results to prevent overwhelming response
            max_results = 50
            if len(search_results) > max_results:
                search_results = search_results[:max_results]
                response["message"] = f"Found {len(search_results)} results (showing top {max_results} by inflation rate)"
            else:
                response["message"] = f"Found {len(search_results)} results"
            
            response["success"] = True
            response["data"] = {
                "query": query,
                "total_results": len(search_results),
                "results": search_results
            }
            
            # Add timing information
            total_elapsed = time.time() - request_start_time
            response["timing"] = {
                "total_seconds": total_elapsed
            }
            
            logger.info(f"Gauge search completed in {total_elapsed:.3f}s for query '{query}' - found {len(search_results)} results")
            return response
            
        except Exception as e:
            logger.error(f"Error in gauge search: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            
            response["message"] = f"Error performing search: {str(e)}"
            return response


# Example of how to use in a Flask or similar framework
"""
from flask import Flask, request, jsonify
from services.gauge_info import GaugeInfoService

app = Flask(__name__)
gauge_service = GaugeInfoService()

# Fast endpoint - local data only (~100-300ms)
@app.route('/api/gauge/basic', methods=['GET'])
def get_basic_gauge_info():
    response = gauge_service.get_basic_gauge_info(request)
    return jsonify(response)

# Medium endpoint - verification only (~1-2s)
@app.route('/api/gauge/verification', methods=['GET'])
def get_gauge_verification():
    response = gauge_service.get_gauge_verification(request)
    return jsonify(response)

# Medium endpoint - boosts only (~0.5-1s)
@app.route('/api/gauge/boosts', methods=['GET'])
def get_gauge_boosts():
    response = gauge_service.get_gauge_boosts(request)
    return jsonify(response)

# Complete endpoint - all data (~2-3s, backward compatibility)
@app.route('/api/gauge/complete', methods=['GET'])
def get_complete_gauge_info():
    response = gauge_service.get_complete_gauge_info(request)
    return jsonify(response)

# Legacy endpoint - backward compatibility
@app.route('/api/gauge', methods=['GET'])
def get_gauge_info():
    response = gauge_service.get_gauge_info(request)
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)
"""

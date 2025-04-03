"""
Utility to check Web3.py version compatibility
"""
from web3 import Web3
import pkg_resources

def check_web3_version():
    """
    Check the installed Web3.py version and print compatibility information
    """
    # Get Web3.py version
    web3_version = pkg_resources.get_distribution("web3").version
    print(f"Installed Web3.py version: {web3_version}")
    
    # Recommended version range
    recommended_version = "5.30.0"
    
    # Check version and print recommendations
    if pkg_resources.parse_version(web3_version) < pkg_resources.parse_version("5.0.0"):
        print("WARNING: You're using an old version of Web3.py (< 5.0.0)")
        print(f"Consider upgrading to version {recommended_version} with:")
        print(f"pip install web3=={recommended_version}")
    elif pkg_resources.parse_version(web3_version) < pkg_resources.parse_version(recommended_version):
        print(f"You're using Web3.py {web3_version}, which should be compatible but not the latest.")
        print(f"Consider upgrading to version {recommended_version} with:")
        print(f"pip install web3=={recommended_version}")
    else:
        print(f"You're using Web3.py {web3_version}, which is up to date.")
    
    # Check eth_abi version
    try:
        eth_abi_version = pkg_resources.get_distribution("eth-abi").version
        print(f"Installed eth-abi version: {eth_abi_version}")
    except pkg_resources.DistributionNotFound:
        print("eth-abi package not found directly. It should be included with Web3.py.")

# Run the check if script is executed directly
if __name__ == "__main__":
    check_web3_version() 
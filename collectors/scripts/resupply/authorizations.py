from pathlib import Path
import json
import requests
from web3 import Web3
from typing import Optional, Dict
from brownie import web3, ZERO_ADDRESS, chain, interface
from .constants import CONTRACTS
from .contract_names import get_contract_name
from utils.utils import contract_creation_block
from config import get_json_path
from utils.feed_chain import event_logs

SELECTORS = None


def normalize_selector(selector_hex: str) -> str:
    """Normalize selector strings to 0x-prefixed, lowercase format."""
    if not selector_hex:
        return selector_hex
    selector_hex = selector_hex.lower()
    if not selector_hex.startswith('0x'):
        selector_hex = f"0x{selector_hex}"
    return selector_hex


def get_selectors_path() -> Path:
    """Helper to get selectors.json path."""
    return Path(get_json_path('selectors.json'))

def get_selectors() -> Dict[str, str]:
    """Load the selectors from selectors.json, caching in memory."""
    global SELECTORS
    if SELECTORS is not None:
        return SELECTORS
    selectors_file = get_selectors_path()
    if not selectors_file.exists():
        SELECTORS = {}
        print("No selectors file found.")
        return SELECTORS
    with open(selectors_file, 'r') as f:
        raw_selectors = json.load(f)
    normalized_selectors = {normalize_selector(k): v for k, v in raw_selectors.items()}
    if normalized_selectors != raw_selectors:
        selectors_file.write_text(json.dumps(normalized_selectors, indent=2))
    SELECTORS = normalized_selectors
    print(f"Loaded {len(SELECTORS)} selectors from {selectors_file}")
    return SELECTORS


def save_selectors(selectors: Dict[str, str]) -> None:
    """Persist selectors to disk and update cache."""
    global SELECTORS
    selectors_file = get_selectors_path()
    selectors_file.parent.mkdir(parents=True, exist_ok=True)
    normalized_selectors = {normalize_selector(k): v for k, v in selectors.items()}
    selectors_file.write_text(json.dumps(normalized_selectors, indent=2))
    SELECTORS = normalized_selectors

def get_function_selector(signature: str) -> str:
    """Generate function selector from function signature"""
    return normalize_selector(Web3.keccak(text=signature)[:4].hex())

def lookup_selector(selector_hex: str) -> Optional[str]:
    """Look up a function signature by its selector in selectors.json"""
    selector_hex = normalize_selector(selector_hex)
    selectors = get_selectors()
    signature = selectors.get(selector_hex)

    if signature:
        return signature

    # Regenerate from local ABIs in case selectors.json is stale
    selectors = generate_selectors()
    signature = selectors.get(selector_hex)
    if signature:
        return signature

    # Fallback to 4byte API; best-effort, ignore failures
    try:
        resp = requests.get(
            "https://www.4byte.directory/api/v1/signatures/",
            params={"hex_signature": selector_hex},
            timeout=5,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            signature = results[0].get("text_signature")
            if signature:
                selectors[selector_hex] = signature
                save_selectors(selectors)
                return signature
    except Exception as exc:
        print(f"4byte lookup failed for {selector_hex}: {exc}")

    return None

def generate_selectors() -> Dict[str, str]:
    """Generate and save selectors from interface JSONs"""
    global SELECTORS
    project_root = Path(__file__).resolve().parents[2]
    interfaces_dir = project_root / "interfaces/resupply"
    selectors_file = get_selectors_path()

    selectors = {}

    # Ensure interfaces directory exists
    if not interfaces_dir.exists():
        print(f"Interfaces directory not found at {interfaces_dir}")
        return selectors

    # Process each .json file in the interfaces directory
    for json_file in interfaces_dir.glob("*.json"):
        contract_name = json_file.stem

        with open(json_file, 'r') as f:
            abi = json.load(f)

            # Find all function entries in the ABI
            for item in abi:
                if item.get('type') == 'function':
                    name = item.get('name')
                    inputs = item.get('inputs', [])

                    # Build function signature
                    param_types = [inp['type'] for inp in inputs]
                    signature = f"{name}({','.join(param_types)})"

                    # Generate and store selector
                    selector = get_function_selector(signature)
                    selectors[selector] = f"{signature}"

    # Save selectors to JSON file
    save_selectors(selectors)

    print(f"Generated selectors file at {selectors_file}")
    print(f"Found {len(selectors)} function selectors")

    # Update global cache
    SELECTORS = selectors

    return selectors

# Only generate selectors if run directly
if __name__ == "__main__":
    generate_selectors()

def get_active_authorizations(logs):
    """Return only currently active authorizations from the log list."""
    # Map: (selector_hex, caller, target) -> last log
    last_state = {}
    for entry in logs:
        key = (entry['selector'][0], entry['caller'], entry['target'])
        # Since logs are sorted newest first, only set if not already set
        if key not in last_state:
            last_state[key] = entry
    # Only keep those where authorized is True
    active = [entry for entry in last_state.values() if entry['authorized']]
    return active

def get_all_selectors(current_height, cached_authorizations, last_processed_block):
    # Load selectors (will use cached version if already loaded)
    get_selectors()

    # Get new events since last processed block
    new_authorizations = []
    if last_processed_block < current_height:
        core = interface.ICore(CONTRACTS["CORE"])

        logs = event_logs(core.events.OperatorSet, last_processed_block + 1, current_height)
        for log in logs:
            selector_hex = web3.to_hex(log.args.selector)
            new_authorizations.append({
                'block': log.blockNumber,
                'txn': '0x' + log.transactionHash.hex(),
                'selector': (selector_hex, ""),
                'caller': log.args.caller,
                'auth_hook': log.args.authHook,
                'authorized': log.args.authorized,
                'target': log.args.target,
                'timestamp': chain[log.blockNumber].timestamp,
                'log_index': int(log.logIndex)
            })

    complete_authorizations = cached_authorizations + new_authorizations
    complete_authorizations.sort(key=lambda x: (x['block'], x.get('log_index', -1)), reverse=True)

    # Lookup selectors and add contract names
    missing_selectors = set()
    for entry in complete_authorizations:
        selector_hex = entry['selector'][0]
        signature = lookup_selector(selector_hex)
        if signature is None:
            missing_selectors.add(selector_hex)
            signature = ""
        entry['selector'] = (selector_hex, signature)

        # Add contract names for caller and target
        entry['caller_name'] = get_contract_name(entry['caller'])
        entry['target_name'] = get_contract_name(entry['target'])

    if missing_selectors:
        print(f"Warning: {len(missing_selectors)} selectors not found in local cache or 4byte.directory")

    active_authorizations = get_active_authorizations(complete_authorizations)
    print(f"Active authorizations: {len(active_authorizations)}")
    return {'all': complete_authorizations, 'active': active_authorizations}

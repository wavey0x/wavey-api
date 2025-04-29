VALIDATOR_ABI = [
    {
        "inputs": [],
        "name": "getActiveProposalDetails",
        "outputs": [
            {
                "components": [
                    {"name": "id", "type": "uint256"},
                    {"name": "gauges", "type": "address[]"},
                    {"name": "executed", "type": "bool"},
                    {"name": "startDate", "type": "uint256"},
                    {"name": "isValid", "type": "bool"}
                ],
                "name": "proposals",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
]
VALIDATOR_ABI = [
    {
        "inputs": [],
        "name": "getActiveProposals",
        "outputs": [
            {
                "name": "proposalIds",
                "type": "uint256[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "proposalId", "type": "uint256"}
        ],
        "name": "getProposalGauges",
        "outputs": [
            {
                "name": "gauges",
                "type": "address[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "proposalId", "type": "uint256"}
        ],
        "name": "validateProposalGauges",
        "outputs": [
            {
                "components": [
                    {"name": "gauge", "type": "address"},
                    {"name": "valid", "type": "bool"}
                ],
                "name": "results",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    {
        "inputs": [
            {"name": "gauge", "type": "address"}
        ],
        "name": "validateGauge",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

DAO_ABI = [
    {
        "inputs": [
            {"name": "voteId", "type": "uint256"}
        ],
        "name": "getVote",
        "outputs": [
            {"name": "open", "type": "bool"},
            {"name": "executed", "type": "bool"},
            {"name": "startDate", "type": "uint64"},
            {"name": "snapshotBlock", "type": "uint64"},
            {"name": "supportRequired", "type": "uint64"},
            {"name": "minAcceptQuorum", "type": "uint64"},
            {"name": "yea", "type": "uint256"},
            {"name": "nay", "type": "uint256"},
            {"name": "votingPower", "type": "uint256"},
            {"name": "script", "type": "bytes"}
        ],
        "stateMutability": "view",
        "type": "function"
    }
]

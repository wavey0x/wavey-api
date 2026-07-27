from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask
from web3 import Web3

from services import crvlol


VALID_GAUGE = "0xbe0451815b546F705ef3f398B8179aE3AADDA14e"
INVALID_GAUGE = "0x055be5DDB7A925BfEF3417FC157f53CA77cA7222"


class Call:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    def call(self):
        if self.error is not None:
            raise self.error
        return self.result


class CoreFunctions:
    def __init__(self, analyses, proposal_ids=None, proposal_error=None):
        self.analyses = analyses
        self.proposal_ids = proposal_ids
        self.proposal_error = proposal_error

    def getActiveProposals(self):
        return Call(self.proposal_ids, self.proposal_error)

    def analyzeProposalGauges(self, proposal_id):
        return Call(self.analyses[proposal_id])


class DAOFunctions:
    def getVote(self, proposal_id):
        return Call(
            (
                True,
                False,
                1_700_000_000 + proposal_id,
                0,
                0,
                0,
                0,
                0,
                0,
                b"",
            )
        )


def _contract(functions):
    return SimpleNamespace(functions=functions)


def _call_endpoint(analyses, proposal_ids=None, proposal_error=None):
    web3 = Web3()
    core = _contract(CoreFunctions(analyses, proposal_ids, proposal_error))
    dao = _contract(DAOFunctions())

    def contract_for(_web3, address, _abi):
        if address == crvlol.GAUGE_VALIDATOR_ADDRESS:
            return core
        if address == crvlol.CURVE_DAO_ADDRESS:
            return dao
        raise AssertionError(f"unexpected contract address: {address}")

    app = Flask(__name__)
    with (
        app.test_request_context(),
        patch.object(crvlol, "setup_web3", return_value=web3),
        patch.object(crvlol, "get_contract", side_effect=contract_for),
    ):
        return crvlol.get_curve_gov_proposals()


def test_returns_every_active_proposal_with_derived_gauge_status():
    response = _call_endpoint(
        {
            10: (True, []),
            11: (False, []),
            12: (True, [(VALID_GAUGE, True)]),
            13: (False, [(INVALID_GAUGE, False)]),
        },
        proposal_ids=[10, 11, 12, 13],
    )

    payload = response.get_json()

    assert response.status_code == 200
    assert [proposal["id"] for proposal in payload["data"]] == [10, 11, 12, 13]
    assert [
        proposal["gaugeValidationStatus"] for proposal in payload["data"]
    ] == ["not_applicable", "unsupported", "valid", "invalid"]
    assert payload["data"][0]["gauges"] == []
    assert payload["data"][2]["gauges"] == [Web3.to_checksum_address(VALID_GAUGE)]
    assert "isValid" not in payload["data"][0]


def test_active_proposal_discovery_failure_is_an_endpoint_error():
    response, status_code = _call_endpoint(
        {},
        proposal_error=RuntimeError("proposal discovery unavailable"),
    )

    assert status_code == 500
    assert response.get_json() == {
        "status": "error",
        "message": "proposal discovery unavailable",
    }

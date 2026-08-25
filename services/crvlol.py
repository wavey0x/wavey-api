from flask import request, jsonify
from models import CrvLlHarvest
import os
from dotenv import load_dotenv
from .web3_services import setup_web3, get_contract
from .abis.validator_abi import DAO_ABI, VALIDATOR_ABI
from .crvlol_snapshot import load_snapshot

load_dotenv()

GAUGE_VALIDATOR_ADDRESS = os.getenv(
    'GAUGE_VALIDATOR_ADDRESS',
    '0x999901076BB47Ae96d135C567610270d006B8684'
)
CURVE_DAO_ADDRESS = os.getenv(
    'CURVE_DAO_ADDRESS',
    '0xE478de485ad2fe566d49342Cbd03E49ed7DB3356'
)


def get_harvests():
    # Get query parameters for pagination
    page = request.args.get('page', 1, type=int)
    page = 1 if page < 1 else page
    per_page = request.args.get('per_page', 20, type=int)
    per_page = 20 if per_page < 1 or per_page > 100 else per_page
    
    # Calculate the offset
    offset = (page - 1) * per_page
    
    # Query the database with pagination
    harvests = CrvLlHarvest.query.order_by(CrvLlHarvest.timestamp.desc()).offset(offset).limit(per_page).all()
    
    # Get the total number of records for pagination metadata
    total = CrvLlHarvest.query.count()
    
    results = [
        {
            "id": harvest.id,
            "profit": str(harvest.profit),
            "timestamp": harvest.timestamp,
            "name": harvest.name,
            "underlying": harvest.underlying,
            "compounder": harvest.compounder,
            "block": harvest.block,
            "txn_hash": harvest.txn_hash,
            "date_str": harvest.date_str
        } for harvest in harvests
    ]
    
    return jsonify({
        'page': page,
        'per_page': per_page,
        'total': total,
        'data': results
    })


def ll_info():
    try:
        data = load_snapshot()
        data.pop('curve_gauge_data', None)
        data.pop('curve_gauges_by_name', None)
        return jsonify(data)
    except FileNotFoundError:
        return jsonify({"error": "CRV snapshot not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_treasury_balance_sheet():
    try:
        data = load_snapshot()
    except FileNotFoundError:
        return jsonify({"error": "CRV snapshot not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    treasury_balance_sheet = data.get('treasury_balance_sheet')
    if not treasury_balance_sheet:
        return jsonify({"error": "treasury_balance_sheet not found in CRV snapshot"}), 404

    return jsonify(treasury_balance_sheet)


def _format_gauge_validations(web3, validations):
    return [
        {
            "gauge": web3.to_checksum_address(validation[0]),
            "valid": bool(validation[1])
        }
        for validation in validations
    ]


def _gauge_validation_status(all_valid, gauge_validations):
    if gauge_validations:
        return "valid" if all_valid else "invalid"
    return "not_applicable" if all_valid else "unsupported"


def get_curve_gov_proposals():
    """
    Fetch every active ownership proposal and its derived gauge-validation
    status.
    
    Returns:
        JSON response containing proposal information
    """
    try:
        # Initialize web3
        web3 = setup_web3()
        validator_contract = get_contract(web3, GAUGE_VALIDATOR_ADDRESS, VALIDATOR_ABI)
        dao_contract = get_contract(web3, CURVE_DAO_ADDRESS, DAO_ABI)
        proposal_ids = validator_contract.functions.getActiveProposals().call()
        
        # Format the response
        formatted_proposals = []
        for proposal_id in proposal_ids:
            all_valid, validations = (
                validator_contract.functions.analyzeProposalGauges(proposal_id).call()
            )
            gauge_validations = _format_gauge_validations(web3, validations)
            vote = dao_contract.functions.getVote(proposal_id).call()

            formatted_proposals.append({
                "id": int(proposal_id),
                "gauges": [validation["gauge"] for validation in gauge_validations],
                "gaugeValidations": gauge_validations,
                "gaugeValidationStatus": _gauge_validation_status(
                    bool(all_valid),
                    gauge_validations
                ),
                "executed": bool(vote[1]),
                "startDate": int(vote[2])
            })
        
        return jsonify({
            "status": "success",
            "data": formatted_proposals
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    

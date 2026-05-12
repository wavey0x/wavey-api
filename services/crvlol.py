from flask import Flask, request, jsonify, send_from_directory
import pandas as pd
from models import CrvLlHarvest
import json, os, glob
from dotenv import load_dotenv
from web3 import Web3
from .web3_services import setup_web3, get_contract
from .abis.validator_abi import DAO_ABI, VALIDATOR_ABI

load_dotenv()

GAUGE_VALIDATOR_ADDRESS = os.getenv(
    'GAUGE_VALIDATOR_ADDRESS',
    '0x79746Fc3275E2ad36597AE0a721DE01DA6878A58'
)
CURVE_DAO_ADDRESS = os.getenv(
    'CURVE_DAO_ADDRESS',
    '0xE478de485ad2fe566d49342Cbd03E49ed7DB3356'
)


def _get_ll_info_path():
    filepath = os.getenv('HOME_DIRECTORY')
    return f'{filepath}/curve-ll-charts/data/ll_info.json'


def _load_ll_info():
    filepath = _get_ll_info_path()
    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath)
    with open(filepath) as file:
        return json.load(file)
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
    filepath = _get_ll_info_path()
    if not glob.glob(filepath):
        return "File not found", 404
    try:
        data = _load_ll_info()
        data.pop('curve_gauge_data', None)
        data.pop('curve_gauges_by_name', None)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_treasury_balance_sheet():
    try:
        data = _load_ll_info()
    except FileNotFoundError:
        return jsonify({"error": "ll_info file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    treasury_balance_sheet = data.get('treasury_balance_sheet')
    if not treasury_balance_sheet:
        return jsonify({"error": "treasury_balance_sheet not found in ll_info.json"}), 404

    return jsonify(treasury_balance_sheet)
# Serve the most recent chart JSON
def get_chart(chart_name, peg):
    peg_str = 'True' if peg.lower() == 'true' else 'False'
    filepath = os.getenv('HOME_DIRECTORY')
    pattern = f'{filepath}/curve-ll-charts/charts/{chart_name}_{peg_str}*.json'
    files = glob.glob(pattern)
    if not files:
        return "File not found", 404
    latest_file = max(files, key=os.path.getctime)
    return send_from_directory(os.path.dirname(latest_file), os.path.basename(latest_file))

def get_curve_gauge_data():
    """
    Get curve gauge data from local file to reduce latency.
    
    Returns:
        JSON response containing curve gauge data
    """
    try:
        filepath = os.getenv('HOME_DIRECTORY')
        filepath = f'{filepath}/curve-ll-charts/data/ll_info.json'
        
        if not os.path.exists(filepath):
            return jsonify({"error": "ll_info file not found"}), 404
            
        with open(filepath) as file:
            data = json.load(file)
            
        # Extract the curve_gauge_data key from ll_info.json
        if "curve_gauge_data" in data:
            gauge_data = data["curve_gauge_data"]
            return jsonify({
                "status": "success",
                "data": gauge_data
            })
        else:
            return jsonify({
                "status": "error",
                "message": "curve_gauge_data key not found in ll_info.json"
            }), 500
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

def _format_gauge_validations(web3, validations):
    return [
        {
            "gauge": web3.to_checksum_address(validation[0]),
            "valid": bool(validation[1])
        }
        for validation in validations
    ]

def get_curve_gov_proposals():
    """
    Fetch active proposals from the Curve governance contract.
    
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
            validations = validator_contract.functions.validateProposalGauges(proposal_id).call()
            gauge_validations = _format_gauge_validations(web3, validations)
            vote = dao_contract.functions.getVote(proposal_id).call()

            formatted_proposals.append({
                "id": int(proposal_id),
                "gauges": [validation["gauge"] for validation in gauge_validations],
                "gaugeValidations": gauge_validations,
                "executed": bool(vote[1]),
                "startDate": int(vote[2]),
                "isValid": bool(gauge_validations) and all(
                    validation["valid"] for validation in gauge_validations
                )
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
    

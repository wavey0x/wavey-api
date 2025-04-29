from flask import Flask, request, jsonify, send_from_directory
import pandas as pd
from models import CrvLlHarvest
import json, os, glob
from dotenv import load_dotenv
from web3 import Web3
from .web3_services import setup_web3, get_contract
from .abis.validator_abi import VALIDATOR_ABI

load_dotenv()

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
    filepath = os.getenv('HOME_DIRECTORY')
    filepath = f'{filepath}/curve-ll-charts/data/ll_info.json'
    files = glob.glob(filepath)
    if not files:
        return "File not found", 404
    try:
        # Open the JSON file and load its contents
        with open(filepath) as file:
            data = json.load(file)
        # Return the JSON data as a response
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
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

def get_curve_gov_proposals():
    """
    Fetch active proposals from the Curve governance contract.
    
    Returns:
        JSON response containing proposal information
    """
    try:
        # Initialize web3
        web3 = setup_web3()
        
        # Governance contract address
        validator_address = "0x60272833edd3f340f6436a8aaa83290c61524c44"
        
        # Get contract instance
        validator_contract = get_contract(web3, validator_address, VALIDATOR_ABI)
        
        # Call getActiveProposalDetails
        proposals = validator_contract.functions.getActiveProposalDetails().call()
        
        # Format the response
        formatted_proposals = []
        for proposal in proposals:
            formatted_proposals.append({
                "id": proposal[0],  # id
                "gauges": [web3.to_checksum_address(gauge) for gauge in proposal[1]],  # gauges
                "executed": proposal[2],  # executed
                "startDate": proposal[3],  # startDate
                "isValid": proposal[4]  # isValid
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
    
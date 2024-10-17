from flask import Flask, jsonify, request
from models import GaugeVoteInfo, db
from sqlalchemy import func
from web3 import Web3


def get_gauge_votes(request):
    page = request.args.get('page', 1, type=int)
    page = 1 if page < 1 else page
    per_page = request.args.get('per_page', 20, type=int)
    per_page = 20 if per_page < 1 or per_page > 100 else per_page
    
    # Calculate the offset
    offset = (page - 1) * per_page
    
    gauge = request.args.get('gauge')
    
    if not gauge:
        return jsonify({"error": "gauge parameter is required"}), 400
    
    try:
        # Convert the input to a checksummed address
        checksummed_gauge = Web3.to_checksum_address(gauge)
    except ValueError:
        # If the conversion fails, it's not a valid Ethereum address
        return jsonify({"error": "Invalid Ethereum address provided"}), 400
    
    # Query the database for records matching the checksummed gauge
    query = GaugeVoteInfo.query.filter(GaugeVoteInfo.gauge == checksummed_gauge)
    
    total = query.count()
    records = query.order_by(GaugeVoteInfo.timestamp.desc()).offset(offset).limit(per_page).all()
    if not records:
        return jsonify({"message": "No records found for the provided gauge."}), 404

    return jsonify({
            'page': page,
            'per_page': per_page,
            'total': total,
            'data': [record.to_dict() for record in records]
        })

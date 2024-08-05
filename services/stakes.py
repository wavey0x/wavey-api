from flask import Flask, jsonify, request
from models import Stake, db

def get_stakes(request):
    results = Stake.query.all()
    return jsonify([stake.to_dict() for stake in results]), 200

def get_stakes_paged(request):
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    
    # Sort by timestamp in descending order before paginating
    query = Stake.query.order_by(Stake.timestamp.desc())  # Assuming 'timestamp' is the column name
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    
    stakes = pagination.items
    return jsonify([stake.to_dict() for stake in stakes]), 200
from flask import Flask, jsonify, request
from models import GaugeVoteInfo, db


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
    
    # Query the database for records matching the gauge and order by timestamp descending
    records = GaugeVoteInfo.query.filter_by(gauge=gauge).order_by(GaugeVoteInfo.timestamp.desc()).offset(offset).limit(per_page).all()
    total = GaugeVoteInfo.query.filter_by(gauge=gauge).count()
    if not records:
        return jsonify({"message": "No records found for the provided gauge."}), 404

    # Convert the records to dictionaries and return as a JSON response
    # return jsonify([record.to_dict() for record in records])
    return jsonify({
            'page': page,
            'per_page': per_page,
            'total': total,
            'data': [record.to_dict() for record in records]
        })
from flask import Flask, request, jsonify
import pandas as pd
from models import UserWeekInfo, CrvLlHarvest
import json

def user_info(request):
    account = request.args.get('account', 1, type=str)
    week_id = request.args.get('week_id', 1, type=int)
    token = request.args.get('token', 1, type=str)
    print(account)
    print(week_id)
    results = UserWeekInfo.query.filter_by(account=account, week_id=week_id, token=token).all()
    
    if not results:
        return jsonify([])
    
    results_json = [user_info.to_dict() for user_info in results]
    return jsonify(results_json)
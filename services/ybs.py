from flask import Flask, request, jsonify
import pandas as pd
from models import UserWeekInfo, CrvLlHarvest
import json

def user_info():
    account = request.args.get('account', 1, type=str)
    week_id = request.args.get('week_id', 1, type=int)
    print(account)
    print(week_id)
    results = UserWeekInfo.query.filter_by(account=account, week_id=week_id).all()
    
    if not results:
        return jsonify([])
    
    results_json = [user_info.to_dict() for user_info in results]
    return jsonify(results_json)
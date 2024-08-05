from flask import Flask, request, jsonify
import pandas as pd
import requests, re
import time

CACHE_TTL = 60 * 30
DATA_URL = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'

df_cache = None
last_updated = None

def get_status():
    global last_updated
    last_updated = 0 if last_updated is None else 0
    current_time = time.time()
    if last_updated + CACHE_TTL < current_time:
        try:
            response = requests.get(DATA_URL)
            if response.status_code == 200:
                data = response.json()
                df_cache = pd.DataFrame(data['data'])
                last_updated = data['last_updated']
            else:
                raise Exception("Failed to fetch data")
        except Exception as e:
            return jsonify({"error": "Internal server error"}), 500
    result = {'last_updated': last_updated, 'time_since': int(current_time - last_updated)}
    return jsonify(result)
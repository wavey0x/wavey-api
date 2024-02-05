from flask import Flask, request, jsonify
import duckdb
import requests, re
import pandas as pd
import time
import logging

app = Flask(__name__)

SIZE_FIELDS = {'fee', 'amount', 'adjusted_amount'}
TABLE_NAME = 'boost_data'
CACHE_TTL = 60 * 30
DATA_URL = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'
VALID_FIELDS = {
    'account', 'adjusted_amount', 'amount', 'boost_delegate', 'fee', 'receiver', 'txn_hash',
    'start_week', 'end_week', 'start_timestamp', 'end_timestamp', 'start_block', 'end_block',
}
RANGE_FIELDS = {
    'start_timestamp', 'end_timestamp', 'start_block', 'end_block', 'start_week', 'end_week',
}
df_cache = None
last_updated = None

# Configure logging
if __name__ != '__main__':
    # Here is how we setup Gunicorn logging handlers.
    gunicorn_logger = logging.getLogger('gunicorn.error') # Works on all log levels
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level) # take the log level config as specified in gunicorn start up script
else:
    # Running in development, configure default logging
    logging.basicConfig(filename='api.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def log_request():
    # No function parameter needed because Flask uses global context variables.
    full_path = request.full_path
    ip_address = request.remote_addr
    app.logger.info(f"{ip_address} | {full_path}")

def fetch_and_cache_data(force):
    global df_cache
    global last_updated
    current_time = time.time()
    if df_cache is None or force or last_updated + CACHE_TTL < current_time:
        response = requests.get(DATA_URL)
        if response.status_code == 200:
            data = response.json()
            df_cache = pd.DataFrame(data['data'])
            last_updated = data['last_updated']
        else:
            raise Exception("Failed to fetch data")
    return df_cache

def build_where_clause(args):
    where_clauses = []
    for field in VALID_FIELDS:
        values = args.getlist(field)  # This gets all values for the field, accommodating arrays
        if values:
            where_clause = handle_field_conditions(field, values)
            if where_clause:
                where_clauses.append(where_clause)
    return " AND ".join(where_clauses)

def handle_field_conditions(field, values):
    if field in RANGE_FIELDS:
        return handle_range_field(field, values[0])  # Assuming single value for range fields
    elif field in SIZE_FIELDS:
        return handle_size_field(field, values[0])  # Assuming single value for size fields
    else:
        return handle_general_field(field, values)

def handle_range_field(field, value):
    base_field = field.replace('start_', '').replace('end_', '')
    base_field = 'system_week' if base_field == 'week' else base_field
    operator = ">=" if "start" in field else "<="
    return f"{base_field} {operator} {pd.to_numeric(value, errors='raise')}"

def handle_size_field(field, value):
    # Regex to match the operators, numeric value, and not equal condition
    match = re.match(r"^(>=|<=|>|<|!=)?(\d+(?:\.\d+)?)$", value)
    if match:
        operator, number = match.groups()
        operator = '=' if not operator else operator  # Default to equality if no operator is provided
        return f"{field} {operator} {number}"
    else:
        raise ValueError(f"Invalid format for size field '{field}': {value}")

def handle_general_field(field, values):
    conditions = []
    for value in values:
        safe_value = value.replace("'", "''")  # Basic sanitization
        if field in ['account', 'boost_delegate', 'receiver']:
            ens_field = f"{field}_ens"
            if value.startswith('!='):
                safe_value = safe_value[2:]
                conditions.append(f"({field} != '{safe_value}' AND {ens_field} != '{safe_value}')")
            else:
                conditions.append(f"({field} = '{safe_value}' OR {ens_field} = '{safe_value}')")
        else:
            if value.startswith('!='):
                conditions.append(f"{field} != '{safe_value}'")
            else:
                conditions.append(f"{field} = '{safe_value}'")
    return " OR ".join(conditions) if conditions else None

@app.route('/status', methods=['GET'])
def get_info():
    log_request()
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

@app.route('/search', methods=['GET'])
def search_records():
    log_request(request)
    try:
        force = True if request.args.get('force', '').lower() == 'true' else False
        where_clause = build_where_clause(request.args)
        df = fetch_and_cache_data(force)
        con = duckdb.connect(database=':memory:')
        con.register(TABLE_NAME, df)
        query = f"SELECT * FROM {TABLE_NAME}"
        if where_clause:
            query += f" WHERE {where_clause}"
        result = con.execute(query).fetchdf()
        return jsonify(result.to_dict(orient='records'))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True)

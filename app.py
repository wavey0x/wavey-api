from flask import Flask, request, jsonify
import duckdb
import requests, re
import pandas as pd
import time
import logging
from web3 import Web3
from web3.exceptions import ContractLogicError
from dotenv import load_dotenv
import os
import warnings

# Suppress DeprecationWarning
warnings.filterwarnings("ignore", category=DeprecationWarning)
app = Flask(__name__)

DOTENV_PATH = os.getenv('DOTENV_PATH')
print('DOTENV_PATH', DOTENV_PATH)
app.logger.info(f"{DOTENV_PATH}")
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)
else:
    load_dotenv()

INFURA_API_KEY = os.getenv('WEB3_INFURA_PROJECT_ID')


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
# else:
#     # Running in development, configure default logging
#     logging.basicConfig(filename='api.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def log_request():
    # No function parameter needed because Flask uses global context variables.
    full_path = request.full_path
    app.logger.info(f"{full_path}")

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

@app.route('/prisma/txns', methods=['GET'])
def search_records():
    log_request()
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


GAUGE_ABI = [
    {"constant": True, "inputs": [], "name": "factory", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [], "name": "lp_token", "outputs": [{"name": "", "type": "address"}], "type": "function"},
]

FACTORY_ABI = [
    {"constant": True, "inputs": [{"name": "pool", "type": "address"}], "name": "get_gauge", "outputs": [{"name": "", "type": "address"}], "type": "function"},
    {"constant": True, "inputs": [{"name": "gauge", "type": "address"}], "name": "is_valid_gauge", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
]

TRUSTED_FACTORIES = [
    '0x6A8cbed756804B16E05E741eDaBd5cB544AE21bf', # Regular
    '0xabC000d88f23Bb45525E447528DBF656A9D55bf5', # Bridge factory
    '0xeF672bD94913CB6f1d2812a6e18c1fFdEd8eFf5c', # root / child gauge factory for fraxtal
    '0x98EE851a00abeE0d95D08cF4CA2BdCE32aeaAF7F', # CurveTwocryptoFactory
]

def is_valid_contract(web3, address):
    return web3.eth.get_code(address) != '0x0'

def get_contract_function_output(web3, address, abi, function_name, args=[]):
    contract = web3.eth.contract(address=address, abi=abi)
    function = getattr(contract.functions, function_name)
    return function(*args).call()

@app.route('/tools/timestamp', methods=['GET'])
@app.route('/tools/ts', methods=['GET'])
def convert_timestamp():
    unix_timestamp = request.args.get('ts') or request.args.get('timestamp')
    if unix_timestamp.lower() == 'now':
        unix_timestamp = int(time.time())
    else:
        try:
            unix_timestamp = int(unix_timestamp)
        except ValueError as e:
            app.logger.error(f"{e}")
            return jsonify({"error": 'Invalid input. Please pass a timestamp as integer.'}), 400
    
    import datetime
    import pytz
    from dateutil.relativedelta import relativedelta

    # Convert Unix timestamp to UTC
    utc_time = datetime.datetime.utcfromtimestamp(unix_timestamp).replace(tzinfo=pytz.utc)

    # Calculate relative time
    now = datetime.datetime.now(pytz.utc)
    if now > utc_time:
        delta = relativedelta(now, utc_time)
        relative_time = f"{abs(delta.years)} years, {abs(delta.months)} months, {abs(delta.days)} days, " \
                        f"{abs(delta.hours)} hours, {abs(delta.minutes)} minutes ago"
    else:
        delta = relativedelta(utc_time, now)
        relative_time = f"in {delta.years} years, {delta.months} months, {delta.days} days, " \
                        f"{delta.hours} hours, {delta.minutes} minutes"
    
    response = {
        'unix_timestamp': unix_timestamp,
        'utc_time': utc_time,
        # 'utc_time_string': utc_time.strftime('%Y-%m-%d %H:%M:%S %Z'),
        'relative_time': relative_time
    }
    return response

@app.route('/curve/verify_gauge', methods=['GET'])
def gauge_check():
    response = {'is_valid': False, 'message': ''}
    address = request.args.get('a') or request.args.get('address')
    if not address or address == '':
        response['message'] = 'No address parameter given.'
        return response
    
    web3 = Web3(Web3.HTTPProvider(f'https://mainnet.infura.io/v3/{INFURA_API_KEY}'))

    if web3.is_address(address) == False:
        response['message'] = 'Invalid Ethereum address.'
        return response

    address = web3.to_checksum_address(address)

    if not is_valid_contract(web3, address):
        response['message'] = "Supplied address is not a valid contract."
        return response

    # Validate factory
    try:
        factory_address = get_contract_function_output(web3, address, GAUGE_ABI, 'factory')
    except ContractLogicError as e:
        response['message'] = "Contract call reverted. Could not discover factory used to deploy this gauge."
        return response
    if factory_address not in TRUSTED_FACTORIES:
        response['message'] = "Factory used to deploy this is not found on trusted list."
        response['is_valid'] = False
        return response

    is_lp_gauge = True
    try:
        lp_token_address = get_contract_function_output(web3, address, GAUGE_ABI, 'lp_token')
    except:
        is_lp_gauge = False

    if is_lp_gauge:
        try:
            gauge_address = get_contract_function_output(web3, factory_address, FACTORY_ABI, 'get_gauge', args=[lp_token_address])
        except:
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            response['is_valid'] = False
            return response
    else:
        try:
            print(factory_address)
            is_valid_gauge = get_contract_function_output(web3, factory_address, FACTORY_ABI, 'is_valid_gauge', args=[address])
        except:
            response['message'] = "Contract call reverted. This likely means that the supplied address is not a valid gauge from the latest factory."
            response['is_valid'] = False
            return response
        if is_valid_gauge:
            response['message'] = "This is a verified factory deployed gauge."
            response['is_valid'] = True
            return response
        else:
            response['message'] = "The factory reports this gauge as invalid."
            response['is_valid'] = False
            return response

    if gauge_address != address:
        response['message'] = "Factory address for this pool does not match supplied address."
    else:
        response['is_valid'] = True
        response['message'] = "This is a verified factory deployed gauge."
    return response

if __name__ == '__main__':
    app.run(debug=True)
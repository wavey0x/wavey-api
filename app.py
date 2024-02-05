from flask import Flask, request, jsonify
import duckdb
import requests, re
import pandas as pd

app = Flask(__name__)

VALID_FIELDS = {
    'account', 'adjusted_amount', 'amount', 'boost_delegate', 'fee', 'receiver', 'txn_hash',
    'start_week', 'end_week', 'start_timestamp', 'end_timestamp', 'start_block', 'end_block',
}

RANGE_FIELDS = {
    'start_timestamp', 'end_timestamp', 'start_block', 'end_block', 'start_week', 'end_week',
}

SIZE_FIELDS = {'fee', 'amount', 'adjusted_amount'}

TABLE_NAME = 'boost_data'
DATA_URL = 'https://raw.githubusercontent.com/wavey0x/open-data/master/raw_boost_data.json'
df_cache = None

def fetch_and_cache_data():
    global df_cache
    if df_cache is None:
        response = requests.get(DATA_URL)
        if response.status_code == 200:
            data = response.json()['data']
            df_cache = pd.DataFrame(data)
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
    print(field,value)
    base_field = field.replace('start_', '').replace('end_', '')
    base_field = 'system_week' if base_field == 'week' else base_field
    operator = ">=" if "start" in field else "<="
    return f"{base_field} {operator} {pd.to_numeric(value, errors='raise')}"

def handle_size_field(field, value):
    # Regex to match the operators and numeric value
    match = re.match(r"^(>=|<=|>|<)?(\d+(?:\.\d+)?)$", value)
    if match:
        operator, number = match.groups()
        operator = '=' if not operator else operator  # Default to equality if no operator is provided
        return f"{field} {operator} {number}"
    else:
        raise ValueError(f"Invalid format for size field '{field}': {value}")


def handle_general_field(field, values):
    safe_values = [value.replace("'", "''") for value in values]  # Basic sanitization
    values_str = ', '.join(f"'{value}'" for value in safe_values)
    if field in ['account', 'boost_delegate', 'receiver']:
        return f"({field} IN ({values_str}) OR {field}_ens IN ({values_str}))"
    return f"{field} IN ({values_str})"

@app.route('/search', methods=['GET'])
def search_records():
    try:
        where_clause = build_where_clause(request.args)
        df = fetch_and_cache_data()
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

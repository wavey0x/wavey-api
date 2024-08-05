import datetime
import pytz, time
from flask import Flask, request, jsonify
from dateutil.relativedelta import relativedelta

def convert_timestamp(unix_timestamp):
    if unix_timestamp.lower() == 'now':
        unix_timestamp = int(time.time())
    else:
        try:
            unix_timestamp = int(unix_timestamp)
        except ValueError as e:
            return jsonify({"error": 'Invalid input. Please pass a timestamp as integer.'}), 400

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
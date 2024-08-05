from flask import Blueprint, request, jsonify, current_app
import services.verify_gauge as verify_gauge
import services.stakes as stakes
import services.time as convert_timestamp
import services.status as status
import services.ybs as ybs
import services.crvlol as crvlol
import json

api = Blueprint('api', __name__)

@api.route('/ybs/stakes', methods=['GET'])
def get_stakes():
    return stakes.get_stakes_paged(request)

@api.route('/curve/verify_gauge', methods=['GET'])
def verify_gauge_route():
    return verify_gauge.verify_gauge(request)

@api.route('/tools/timestamp', methods=['GET'])
def timestamp_route():
    try:
        return convert_timestamp(request)
    except ValueError as e:
        current_app.logger.error(f"{e}")
        return jsonify({"error": 'Something went wrong.'}), 400

@api.route('/status', methods=['GET'])
def get_status():
    return status.get_status()

@api.route('/ybs/user_info', methods=['GET'])
def get_user_info():
    return ybs.user_info(request)

@api.route('/crvlol/harvests', methods=['GET'])
def get_harvests():
    return crvlol.get_harvests()

@api.route('/crvlol/info')
def ll_info():
    return crvlol.ll_info(request)

@api.route('/crvlol/charts/<chart_name>/<peg>')
def get_chart(chart_name, peg):
    return crvlol.get_chart(chart_name, peg)
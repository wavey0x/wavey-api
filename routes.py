from flask import Blueprint, request, jsonify, current_app
import services.verify_gauge as verify_gauge
import services.stakes as stakes
import services.time as time_module
import services.status as status
import services.ybs as ybs
import services.crvlol as crvlol
import services.gauge_voting as gauge_voting
from services.gauge_info import GaugeInfoService

api = Blueprint('api', __name__)

# Initialize the GaugeInfoService
gauge_info_service = GaugeInfoService()

@api.route('/ybs/stakes', methods=['GET'])
def get_stakes():
    return stakes.get_stakes_paged(request)

@api.route('/curve/verify_gauge', methods=['GET'])
def verify_gauge_route():
    return verify_gauge.verify_gauge(request)

@api.route('/curve/gauge_info', methods=['GET'])
def gauge_info_route():
    """Get detailed information about a gauge"""
    response = gauge_info_service.get_gauge_info(request)
    return jsonify(response)

@api.route('/tools/timestamp', methods=['GET'])
@api.route('/tools/ts', methods=['GET'])
def timestamp_route():
    unix_timestamp = request.args.get('ts') or request.args.get('timestamp')
    try:
        return time_module.convert_timestamp(unix_timestamp)
    except ValueError as e:
        current_app.logger.error(f"{e}")
        return jsonify({"error": 'Something went wrong.'}), 400

@api.route('/status', methods=['GET'])
def get_status():
    return status.get_status()

@api.route('/ybs/user_info', methods=['GET'])
def get_user_info():
    return ybs.user_info(request)

@api.route('/ybs/global_info', methods=['GET'])
def get_global_info():
    return ybs.global_info(request)

@api.route('/crvlol/harvests', methods=['GET'])
def get_harvests():
    return crvlol.get_harvests()

@api.route('/crvlol/info')
def ll_info():
    return crvlol.ll_info()

@api.route('/crvlol/charts/<chart_name>/<peg>')
def get_chart(chart_name, peg):
    return crvlol.get_chart(chart_name, peg)

@api.route('/crvlol/gauge_votes', methods=['GET'])
def get_gauge_votes():
    return gauge_voting.get_gauge_votes(request)

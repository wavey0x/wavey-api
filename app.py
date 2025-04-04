from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import routes
from database import db
from config import Config
from flask_cors import CORS
from services.gauge_info import GaugeInfoService
from routes import api

app = Flask(__name__)
CORS(app)
app.config.from_object(Config())
db.init_app(app)

app.register_blueprint(api, url_prefix='/api')

# Create a single gauge service instance for the app
gauge_service = GaugeInfoService()

# Root level endpoint for gauge info - this is what your frontend is calling
@app.route('/', methods=['GET'])
def root():
    if 'gauge' in request.args:
        response = gauge_service.get_gauge_info(request)
        return jsonify(response)
    return jsonify({"message": "Welcome! Use /?gauge=<address> to get gauge info, or /api for API endpoints."})

# Existing gauge route at /api/gauge
@app.route('/api/gauge', methods=['GET'])
def get_gauge_info():
    response = gauge_service.get_gauge_info(request)
    return jsonify(response)

# Also add it under the curve namespace for consistency
@app.route('/api/curve/gauge', methods=['GET'])
def get_curve_gauge_info():
    response = gauge_service.get_gauge_info(request)
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001, debug=True)
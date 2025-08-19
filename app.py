from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import routes
import logging
from logging.handlers import RotatingFileHandler
from database import db
from config import Config
from flask_cors import CORS
from services.gauge_info import GaugeInfoService
from routes import api
import time

CACHE_EXPIRATION_SECONDS = 100

# Configure logging with rotation
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Add rotating file handler with 10MB max size and 5 backup files
file_handler = RotatingFileHandler(
    'app.log', 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)
app.config.from_object(Config())
db.init_app(app)

# Log startup information
logger.info("Starting application")

app.register_blueprint(api, url_prefix='/api')

# Create a single gauge service instance for the app
gauge_service = GaugeInfoService()

# Root level endpoint for gauge info - this is what your frontend is calling
@app.route('/', methods=['GET'])
def root():
    start_time = time.time()
    if 'gauge' in request.args:
        response = gauge_service.get_gauge_info(request)
        elapsed = time.time() - start_time
        logger.info(f"Root route with gauge parameter completed in {elapsed:.3f}s")
        return jsonify(response)
    return jsonify({"message": "Welcome! Use /?gauge=<address> to get gauge info, or /api for API endpoints."})

# Existing gauge route at /api/gauge
@app.route('/api/gauge', methods=['GET'])
def get_gauge_info():
    start_time = time.time()
    response = gauge_service.get_gauge_info(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/gauge route completed in {elapsed:.3f}s")
    return jsonify(response)

# New fast endpoint - basic gauge info only (~100-300ms)
@app.route('/api/gauge/basic', methods=['GET'])
def get_basic_gauge_info():
    start_time = time.time()
    response = gauge_service.get_basic_gauge_info(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/gauge/basic route completed in {elapsed:.3f}s")
    return jsonify(response)

# New endpoint - gauge verification only (~1-2s)
@app.route('/api/gauge/verification', methods=['GET'])
def get_gauge_verification():
    start_time = time.time()
    response = gauge_service.get_gauge_verification(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/gauge/verification route completed in {elapsed:.3f}s")
    return jsonify(response)

# New endpoint - gauge boosts only (~0.5-1s)
@app.route('/api/gauge/boosts', methods=['GET'])
def get_gauge_boosts():
    start_time = time.time()
    response = gauge_service.get_gauge_boosts(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/gauge/boosts route completed in {elapsed:.3f}s")
    return jsonify(response)

# New endpoint - complete gauge info (~2-3s)
@app.route('/api/gauge/complete', methods=['GET'])
def get_complete_gauge_info():
    start_time = time.time()
    response = gauge_service.get_complete_gauge_info(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/gauge/complete route completed in {elapsed:.3f}s")
    return jsonify(response)

# Also add it under the curve namespace for consistency
@app.route('/api/curve/gauge', methods=['GET'])
def get_curve_gauge_info():
    start_time = time.time()
    response = gauge_service.get_gauge_info(request)
    elapsed = time.time() - start_time
    logger.info(f"/api/curve/gauge route completed in {elapsed:.3f}s")
    return jsonify(response)

# Add these endpoints for cache management
@app.route('/api/admin/cache/stats', methods=['GET'])
def cache_stats():
    """Get cache statistics"""
    if request.headers.get('X-Admin-Key') != app.config.get('ADMIN_API_KEY'):
        return jsonify({"error": "Unauthorized"}), 401
        
    stats = gauge_service.get_cache_stats()
    return jsonify({
        "cache_stats": stats,
        "config": {
            "cache_expiration_seconds": CACHE_EXPIRATION_SECONDS
        }
    })

@app.route('/api/admin/cache/refresh', methods=['POST'])
def refresh_cache():
    """Force refresh the cache"""
    if request.headers.get('X-Admin-Key') != app.config.get('ADMIN_API_KEY'):
        return jsonify({"error": "Unauthorized"}), 401
        
    result = gauge_service.force_refresh_cache()
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001, debug=True)
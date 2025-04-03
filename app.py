from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import routes
from database import db
from config import Config
from flask_cors import CORS
from services.gauge_info import GaugeInfoService

app = Flask(__name__)
CORS(app)
app.config.from_object(Config())
db.init_app(app)

app.register_blueprint(routes.api)

gauge_service = GaugeInfoService()

@app.route('/api/gauge', methods=['GET'])
def get_gauge_info():
    response = gauge_service.get_gauge_info(request)
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
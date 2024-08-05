from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import routes
from database import db
from config import Config
from flask_cors import CORS

def create_app():
    app = Flask(__name__)
    CORS(app)
    app.config.from_object(Config())
    db.init_app(app)
    
    app.register_blueprint(routes.api)

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
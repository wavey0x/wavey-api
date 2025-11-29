from database import db
from config import Config
from flask import Flask
from sqlalchemy import inspect

app = Flask(__name__)
app.config.from_object(Config())
db.init_app(app)

with app.app_context():
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()
    print('Available tables:')
    print(tables)
    
    if 'rsup_incentives' in tables:
        print('\nColumns in rsup_incentives:')
        columns = inspector.get_columns('rsup_incentives')
        for col in columns:
            print(f"  {col['name']}: {col['type']}, nullable={col['nullable']}")
    else:
        print('\nTable rsup_incentives not found!')

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event, text
import json

db = SQLAlchemy()

def _require_patched_runtime(runtime):
    patched = (runtime >= (3, 51, 3) or (3, 50, 7) <= runtime < (3, 51, 0)
               or (3, 44, 6) <= runtime < (3, 45, 0))
    if not patched:
        raise RuntimeError('The loaded SQLite runtime needs the WAL-reset fix')

def init_app(app, *, allow_rehearsal=False):
    """Open only the explicitly selected, validated SQLite import."""
    if allow_rehearsal and not app.testing:
        raise RuntimeError('Rehearsal databases may only be opened by test applications')
    db.init_app(app)
    with app.app_context():
        engine = db.engine
        if engine.dialect.name != 'sqlite':
            raise RuntimeError('The shared application database must be SQLite')
        if engine.url.query.get('mode') != 'ro' or engine.url.query.get('uri') != 'true':
            raise RuntimeError('The shared SQLite database must use URI mode=ro')
        if not allow_rehearsal:
            _require_patched_runtime(engine.dialect.dbapi.sqlite_version_info)

        def configure(connection, _record):
            cursor = connection.cursor()
            try:
                cursor.execute('PRAGMA query_only=ON')
                cursor.execute('PRAGMA foreign_keys=ON')
                cursor.execute('PRAGMA busy_timeout=5000')
            finally:
                cursor.close()

        event.listen(engine, 'connect', configure)
        with engine.connect() as connection:
            version = connection.execute(text('PRAGMA user_version')).scalar_one()
            if version != 1:
                raise RuntimeError('Unsupported shared SQLite schema version')
            manifest = json.loads(connection.execute(text(
                "SELECT value FROM _migration_meta WHERE key='manifest'"
            )).scalar_one())
            if manifest.get('snapshot_sha256') != app.config.get('YEARN_IMPORT_SHA256'):
                raise RuntimeError('Shared SQLite import identity does not match configuration')
            if manifest.get('schema_version') != version:
                raise RuntimeError('Shared SQLite manifest schema version does not match')
            if not manifest.get('application_ready'):
                if not allow_rehearsal or manifest.get('status') != 'data_verified':
                    raise RuntimeError('Shared SQLite import is not ready for application use')
            elif manifest.get('status') not in ('ready', 'active'):
                raise RuntimeError('Shared SQLite import has an inconsistent readiness marker')
            journal = connection.execute(text('PRAGMA journal_mode')).scalar_one()
            if not allow_rehearsal and journal != 'wal':
                raise RuntimeError('The live shared SQLite database must use WAL')
            if journal == 'wal':
                runtime = tuple(int(part) for part in connection.execute(
                    text('SELECT sqlite_version()')).scalar_one().split('.'))
                _require_patched_runtime(runtime)

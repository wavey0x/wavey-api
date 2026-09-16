"""Short SQLite transactions against an explicitly prepared shared import."""

from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3
import time


def require_patched_runtime(version=sqlite3.sqlite_version_info):
    if not (version >= (3, 51, 3) or (3, 50, 7) <= version < (3, 51, 0)
            or (3, 44, 6) <= version < (3, 45, 0)):
        raise RuntimeError('The loaded SQLite runtime needs the WAL-reset fix')


class Store:
    def __init__(self, path, import_sha256, *, rehearsal=False):
        self.path = Path(path)
        if not self.path.is_absolute() or not re.fullmatch('[0-9a-f]{64}', import_sha256):
            raise ValueError('An absolute YEARN_DB_PATH and YEARN_IMPORT_SHA256 are required')
        self.import_sha256 = import_sha256
        self.rehearsal = rehearsal
        require_patched_runtime()
        with closing(self.connect()) as connection:
            self._validate(connection)

    @classmethod
    def from_env(cls):
        return cls(os.environ['YEARN_DB_PATH'], os.environ['YEARN_IMPORT_SHA256'])

    def connect(self):
        connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True,
                                     timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA synchronous=FULL')
            connection.execute('PRAGMA busy_timeout=5000')
        except BaseException:
            connection.close()
            raise
        return connection

    def _validate(self, connection):
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        row = connection.execute("SELECT value FROM _migration_meta WHERE key='manifest'").fetchone()
        manifest = json.loads(row[0]) if row else {}
        if version != 1 or manifest.get('schema_version') != version:
            raise RuntimeError('Unsupported shared SQLite schema version')
        if manifest.get('snapshot_sha256') != self.import_sha256:
            raise RuntimeError('Shared SQLite import identity does not match configuration')
        if self.rehearsal:
            if manifest.get('application_ready') or manifest.get('status') != 'data_verified':
                raise RuntimeError('Rehearsal requires an inactive data-verified import')
            if manifest.get('alerts_enabled') is not False:
                raise RuntimeError('Rehearsal requires alerts disabled')
        elif not manifest.get('application_ready') or manifest.get('status') not in ('ready', 'active'):
            raise RuntimeError('Shared SQLite import is not ready for application use')
        if not self.rehearsal and connection.execute('PRAGMA journal_mode').fetchone()[0] != 'wal':
            raise RuntimeError('The live shared SQLite database must use WAL')

    def read(self, callback):
        with closing(self.connect()) as connection:
            self._validate(connection)
            connection.execute('PRAGMA query_only=ON')
            return callback(connection)

    def write(self, callback):
        """Retry only database work. The callback must have no external side effects."""
        for attempt in range(3):
            with closing(self.connect()) as connection:
                try:
                    connection.execute('BEGIN IMMEDIATE')
                    self._validate(connection)
                    result = callback(connection)
                    connection.commit()
                    return result
                except sqlite3.OperationalError as error:
                    connection.rollback()
                    code = getattr(error, 'sqlite_errorcode', 0) & 0xff
                    if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or attempt == 2:
                        raise
                except BaseException:
                    connection.rollback()
                    raise
            time.sleep(0.1 * (attempt + 1))

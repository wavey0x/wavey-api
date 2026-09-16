# wavey-api

Shared Flask API for Wavey services, including Curve gauge tools, CRV.LOL,
liquid-locker harvests, YBS, Resupply, Tidal, and timestamp routes.

CRV.LOL snapshot-backed routes read the file named by the required
`CRVLOL_SNAPSHOT_PATH` environment variable. Production uses
`/var/lib/crv-lol/snapshot.json`; the `crv-lol` refresh service is the only
writer.

The gist service is deployed and maintained separately from this repository.
The production snapshot environment is recorded in
`deploy/systemd/wavey-api.service.d/crv-lol-snapshot.conf`.

Scheduled Resupply and YBS feeds are collected by the maintained Brownie project
under [collectors](collectors/README.md), stored in the shared SQLite database and
served at `/api/resupply/data` and `/api/ybs/data`. Apply its explicit additive
import before starting this API version. The old `/api/status`, Git-published
feeds, Prisma output and position monitoring are retired.

## SQLite database migration

The shared application database now uses an explicitly selected local SQLite
file. Set `YEARN_DB_PATH` to its absolute path and `YEARN_IMPORT_SHA256` to the
validated source snapshot identifier recorded in `_migration_meta`. The old
`DATABASE_URI` setting is not a fallback. Keep unrelated application settings,
including Tidal and CRV.LOL snapshot paths, as before.

Startup opens SQLite with URI `mode=ro`, enables query-only connections, and
checks the import identity, schema version, and application-readiness marker.
A missing file is never created. Production also requires WAL and a loaded
SQLite runtime containing the WAL-reset fix. Do not deploy this branch against
the old PostgreSQL configuration or mark a live rehearsal snapshot ready.

Exact numeric columns are stored as text and decoded as `Decimal`. Existing
JSON number responses remain numbers; harvest profit strings retain their
original numeric scale. The shared importer recreates the existing YBS view.

Run the tests and, against the private frozen source export plus its imported
copy, the database-only verification command. Use a runtime containing the
SQLite fix; preparation was tested with Python 3.12.14 and SQLite 3.53.1 in an
isolated migration environment. `requirements.lock` pins the existing
production application dependencies for Python 3.12, avoiding unrelated
library upgrades during this migration; install it with hash verification:

```sh
python -m pip install --require-hashes -r requirements.lock
python -m pip install -r requirements-dev.txt
python -m pytest -q
python scripts/verify_sqlite_import.py --snapshot /absolute/private/snapshot.jsonl.gz --database /absolute/private/yearn-rehearsal.sqlite3
```

The verification command runs in a Flask test application, opens the database
read-only, and compares all API-backed records, including the YBS join, with the
frozen source. It does not start the server or contact public alert channels.
Database snapshots and credentials must stay outside Git.

Follow the [service-by-service cutover plan](https://gist.wavey.info/RsZwJygIE49CML9UeqHdTn0l)
before switching the production service. Other writers must be stopped for the
final shared copy and brought back using the same SQLite import.

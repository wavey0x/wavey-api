# wavey-api

Shared Flask API for Wavey services, including Curve gauge tools, CRV.LOL,
liquid-locker harvests, YBS, Resupply, Tidal, status, and timestamp routes.

CRV.LOL snapshot-backed routes read the file named by the required
`CRVLOL_SNAPSHOT_PATH` environment variable. Production uses
`/var/lib/crv-lol/snapshot.json`; the `crv-lol` refresh service is the only
writer.

The gist service is deployed and maintained separately from this repository.
The production snapshot environment is recorded in
`deploy/systemd/wavey-api.service.d/crv-lol-snapshot.conf`.

## Prisma Shadow Logs API

This document outlines the usage of the Prisma Shadow Logs API, which allows users to query a dataset of pre-fetched shadow logs from Prisma's vault contract. This API exposes data otherwise difficult to obtain from a regular Ethereum node. The API supports flexible queries with options for range filters, size comparisons, and matching multiple values.

```
{
    'account': "0xA42E8825104635253C64086b34F64057789f65eC", // Also searchable via reverse ENS
    'adjusted_amount': 171.84241873206764,
    'amount': 171.84241873206764,
    'block': 18480795,
    'boost_delegate': "0x0000000000000000000000000000000000000000", // Also searchable via reverse ENS
    'date_str': "11/02/2023, 00:20:35",
    'fee': 0.0,
    'receiver': "0xA42E8825104635253C64086b34F64057789f65eC", // Also searchable via reverse ENS
    'system_week': 12,
    'timestamp': 1698884435,
    'txn_hash': "0x2df70acb3410be009cbea53434f52eddcc7183838567d639e868d0ce7b550b25"
}
```

#### Base URL
`http://<your-server-address>:<port>/search`

Replace <your-server-address> and <port> with the actual address and port where your Flask application is running. Typically, for local development, this would be http://127.0.0.1:5000/search.

Demo available at [https://api.wavey.info/search?end_timestamp=1707100000](https://api.wavey.info/search?end_timestamp=1707100000)

### Supported Query Parameters

Replace `<your-server-address>` and `<port>` with the actual address and port where your Flask application is running. Typically, for local development, this would be `http://127.0.0.1:5000/search`.

#### Supported Query Parameters

- `account`, `boost_delegate`, `receiver`: Specify one or more account identifiers. To query multiple values, repeat the parameter with different values.
  
- `amount`, `adjusted_amount`, `fee`: Specify numeric values to match or use operators (`>`, `>=`, `<`, `<=`) for comparison. Multiple conditions can be applied by repeating the parameter with different values.

- `start_week`, `end_week`, `start_timestamp`, `end_timestamp`, `start_block`, `end_block`: Specify start and end values to define a range. For week, timestamp, and block identifiers, use `start_` or `end_` prefixes to indicate range boundaries.

- `txn_hash`: Specify a transaction hash to match specific transactions.

### Examples
#### Query by Single Account

```sql
GET /search?account=0x123
```
Query with Amount Greater Than

```sql
GET /search?amount=>100
```
Query by Multiple Receivers

```sql
GET /search?receiver=0x123&receiver=0x456
```
Range Query for Timestamp

```sql
GET /search?start_timestamp=1609459200&end_timestamp=1612137600
```
Combining Filters with Different Conditions


```sql
GET /search?start_week=1&end_week=52&amount=>=100&amount=<=500&account=0x123
```

### Response Format
The response will be in JSON format, containing an array of records that match the query parameters. Each record includes all fields from the dataset that match the criteria.

### Example:

```json
[
  {
    "account": "0x123",
    "adjusted_amount": 150,
    "amount": 200,
    "boost_delegate": "0x456",
    "fee": 10,
    "receiver": "0x789",
    "txn_hash": "abc123",
    "system_week": 12,
    "timestamp": 1610000000,
    "block": 123456
  }
  // More records...
]
```

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

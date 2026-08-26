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

## Proposal Trace API

The optional Proposal Trace routes read its contract-version-1 SQLite database
through short-lived `mode=ro`, `query_only` connections. Proposal Trace remains
the sole writer and does not depend on this service. Set
`PROPOSAL_TRACE_DB_PATH` to the canonical absolute database path and optionally
set `PROPOSAL_TRACE_BUSY_TIMEOUT_MS` (default `5000`).

```text
GET /api/proposal-trace/<source-path>/audits
GET /api/proposal-trace/<source-path>/audits/<external-id>
GET /api/proposal-trace/safe/<chain-id>/<safe-address>/audits/by-nonce/<nonce>
```

The reviewed policies in `services/proposal_trace_policy.py` are complete for
the tracked source set and fail closed for every unknown source. These sources
are public because their reports are already public Wavey Gists. An
`authenticated` policy requires a separate `PROPOSAL_TRACE_API_KEY` and the
`X-Proposal-Trace-Key` request header; it never reuses the admin credential.

The co-located Claw deployment runs the same routes in the minimal
`proposal_trace_app.py` WSGI application on `127.0.0.1:3101`. It uses the
dedicated `wavey-api` account and `proposal-trace-reader` group, which can
traverse the state directory and read only `state.db`; it cannot list that
directory or access Proposal Trace configuration, workspaces, Codex state, or
delivery capabilities. Bootstrap once, then use the same command without the
flag for later releases:

```text
ops/deploy-proposal-trace-api.sh claw --bootstrap
ops/deploy-proposal-trace-api.sh claw
```

This loopback deployment deliberately does not claim the public
`api.wavey.info` hostname. Public routing is a separate host/DNS decision.

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

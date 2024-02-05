### API Documentation: Search Records API

This document outlines the usage of the Search Records API, which allows users to query a dataset based on various criteria including accounts, amounts, time ranges, and more. The API supports flexible queries with options for range filters, size comparisons, and matching multiple values.

#### Base URL
http://<your-server-address>:<port>/search
Replace <your-server-address> and <port> with the actual address and port where your Flask application is running. Typically, for local development, this would be http://127.0.0.1:5000/search.

### Supported Query Parameters

Replace `<your-server-address>` and `<port>` with the actual address and port where your Flask application is running. Typically, for local development, this would be `http://127.0.0.1:5000/search`.

#### Supported Query Parameters

- `account`, `boost_delegate`, `receiver`: Specify one or more account identifiers. To query multiple values, repeat the parameter with different values.
  
- `amount`, `adjusted_amount`, `fee`: Specify numeric values to match or use operators (`>`, `>=`, `<`, `<=`) for comparison. Multiple conditions can be applied by repeating the parameter with different values.

- `start_week`, `end_week`, `start_timestamp`, `end_timestamp`, `start_block`, `end_block`: Specify start and end values to define a range. For week, timestamp, and block identifiers, use `start_` or `end_` prefixes to indicate range boundaries.

- `txn_hash`: Specify a transaction hash to match specific transactions.

#### Examples

1. **Query by Single Account**
   


###Examples
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
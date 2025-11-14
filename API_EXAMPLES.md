# Etherduck API - curl Examples

This document provides curl examples for testing the Etherduck REST API endpoints.

## Base URL

The API runs on `http://localhost:9988` by default.

## Endpoints

### 1. List Events

Get a list of all event descriptors in the database.

**Endpoint:** `POST /list_events`

**Request Body:** Empty JSON object `{}` (or no body)

**Example:**

```bash
# With empty JSON body
curl -X POST http://localhost:9988/list_events \
  -H "Content-Type: application/json" \
  -d '{}'

# Or simply (some frameworks accept empty POST)
curl -X POST http://localhost:9988/list_events \
  -H "Content-Type: application/json"
```

**Expected Response:**

```json
{
  "events": [
    {
      "event_signature": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "event_name": "Transfer",
      "event_type": "ERC20"
    }
  ]
}
```

---

### 2. List Contracts

Get a list of all contracts in the database.

**Endpoint:** `POST /list_contracts`

**Request Body:** Empty JSON object `{}` (or no body)

**Example:**

```bash
curl -X POST http://localhost:9988/list_contracts \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Expected Response:**

```json
{
  "contracts": [
    {
      "contract_address": "event_0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "contract_name": null
    }
  ]
}
```

---

### 3. Get Events

Retrieve events for a specific contract within a time range.

**Endpoint:** `POST /get_events`

**Request Body:**

```json
{
  "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
  "start_time": "2024-01-01T00:00:00Z",
  "end_time": "2024-01-02T00:00:00Z",
  "event": "optional_event_string"
}
```

**Parameters:**
- `contract` (required): Contract address (hex string with 0x prefix)
- `start_time` (required): Start time in RFC3339 format (ISO 8601)
- `end_time` (optional): End time in RFC3339 format (ISO 8601). If not provided, uses current time
- `event` (optional): Event identifier (currently not used in implementation)

**Examples:**

#### Get events for the last 24 hours

```bash
# Calculate timestamps (Unix timestamp approach)
START_TIME=$(date -u -d '24 hours ago' +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d "{
    \"contract\": \"0xdAC17F958D2ee523a2206206994597C13D831ec7\",
    \"start_time\": \"$START_TIME\",
    \"end_time\": \"$END_TIME\"
  }"
```

#### Get events for the last 7 days (USDC contract)

```bash
START_TIME=$(date -u -d '7 days ago' +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d "{
    \"contract\": \"0xdAC17F958D2ee523a2206206994597C13D831ec7\",
    \"start_time\": \"$START_TIME\",
    \"end_time\": \"$END_TIME\"
  }"
```

#### Get events for a specific date range

```bash
curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d '{
    "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "start_time": "2024-01-15T00:00:00Z",
    "end_time": "2024-01-16T00:00:00Z"
  }'
```

#### Get events without end_time (uses current time)

```bash
curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d '{
    "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "start_time": "2024-01-15T00:00:00Z"
  }'
```

#### Get events for the last 5 minutes

```bash
START_TIME=$(date -u -d '5 minutes ago' +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d "{
    \"contract\": \"0xdAC17F958D2ee523a2206206994597C13D831ec7\",
    \"start_time\": \"$START_TIME\",
    \"end_time\": \"$END_TIME\"
  }"
```

**Expected Response:**

```json
{
  "events": [
    {
      "block_number": 12345678,
      "transaction_hash": "0xabc123...",
      "log_index": 0,
      "contract_address": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
      "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "topic1": "0x0000000000000000000000001234567890123456789012345678901234567890",
      "topic2": "0x0000000000000000000000000987654321098765432109876543210987654321",
      "topic3": null,
      "block_timestamp": "2024-01-15T12:34:56Z"
    }
  ]
}
```

---

### 4. Raw Query

Execute a raw SQL query against the database.

**Endpoint:** `POST /raw_query`

**Request Body:**

```json
{
  "query": "SELECT * FROM blocks LIMIT 10"
}
```

**Parameters:**
- `query` (required): SQL SELECT query string

**Examples:**

#### Simple SELECT query

```bash
curl -X POST http://localhost:9988/raw_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM blocks LIMIT 10"
  }'
```

#### Query with WHERE clause

```bash
curl -X POST http://localhost:9988/raw_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT block_number, block_timestamp FROM blocks WHERE block_number > 23780000 ORDER BY block_number DESC LIMIT 5"
  }'
```

#### Query event tables

```bash
curl -X POST http://localhost:9988/raw_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT COUNT(*) as total_events FROM event_0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
  }'
```

**Expected Response:**

The response format depends on the query result. It will be a JSON object with a `result` field containing the query results:

```json
{
  "result": {
    "error": "Empty result from the query"
  }
}
```

Or when the query returns data (once fully implemented):

```json
{
  "result": [
    {
      "block_number": 23783860,
      "block_hash": "0x...",
      "block_timestamp": 1731420743
    }
  ]
}
```

**Note:** Currently, the implementation returns an error message. The actual query execution will be implemented in a future update.

---

## Error Responses

All endpoints return errors in the following format:

```json
{
  "error": "Error message description"
}
```

**Common HTTP Status Codes:**
- `200 OK`: Success
- `400 Bad Request`: Invalid request parameters (e.g., invalid contract address or timestamp format)
- `500 Internal Server Error`: Server-side error (e.g., database connection issue)

**Example Error Response:**

```bash
# Invalid contract address
curl -X POST http://localhost:9988/get_events \
  -H "Content-Type: application/json" \
  -d '{
    "contract": "invalid_address",
    "start_time": "2024-01-15T00:00:00Z"
  }'
```

Response:
```json
{
  "error": "Invalid contract address: ..."
}
```

---

## Pretty-printing JSON Responses

To make the responses more readable, pipe through `jq`:

```bash
curl -X POST http://localhost:9988/list_events \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.'
```

Or use Python:

```bash
curl -X POST http://localhost:9988/list_events \
  -H "Content-Type: application/json" \
  -d '{}' | python3 -m json.tool
```

---

## Testing Script

Here's a simple bash script to test all endpoints:

```bash
#!/bin/bash

API_URL="http://localhost:9988"

echo "=== Testing List Events ==="
curl -X POST $API_URL/list_events \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.'

echo -e "\n=== Testing List Contracts ==="
curl -X POST $API_URL/list_contracts \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.'

echo -e "\n=== Testing Get Events (Last 24 hours) ==="
START_TIME=$(date -u -d '24 hours ago' +"%Y-%m-%dT%H:%M:%SZ")
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

curl -X POST $API_URL/get_events \
  -H "Content-Type: application/json" \
  -d "{
    \"contract\": \"0xdAC17F958D2ee523a2206206994597C13D831ec7\",
    \"start_time\": \"$START_TIME\",
    \"end_time\": \"$END_TIME\"
  }" | jq '.'

echo -e "\n=== Testing Raw Query ==="
curl -X POST $API_URL/raw_query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM blocks LIMIT 5"
  }' | jq '.'
```

Save this as `test_api.sh`, make it executable (`chmod +x test_api.sh`), and run it.

---

## Notes

- All timestamps must be in RFC3339 format (ISO 8601): `YYYY-MM-DDTHH:MM:SSZ`
- Contract addresses must be valid hex strings with `0x` prefix
- The API server must be running before making requests
- All endpoints use POST method and expect JSON content type


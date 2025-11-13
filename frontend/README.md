# Etherduck Frontend Dashboard

A Streamlit-based web dashboard for visualizing USDC Transfer events from the Etherduck indexer.

## Features

- **Events Over Time Graph**: Shows the number of Transfer events per hour for the last 7 days
- **Top 5 Addresses Table**: Displays the top 5 addresses with the most transactions (from topic1) in the last 24 hours
- **Real-time Gauges**: Four gauge indicators showing event counts for:
  - Last 5 minutes
  - Last 30 minutes
  - Last 1 hour
  - Last 24 hours

## Prerequisites

- Python 3.8 or higher (Python 3.13 supported)
- The Etherduck API server running on `http://localhost:9988`

## Installation

1. Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Running the Dashboard

1. Make sure the Etherduck API server is running (see the main project README)

2. Start the Streamlit application:

```bash
streamlit run app_streamlit.py
```

Or if you prefer to specify the port:

```bash
streamlit run app_streamlit.py --server.port 8050
```

3. The dashboard will automatically open in your default web browser, or navigate to:

```
http://localhost:8501
```

(Streamlit defaults to port 8501, but you can change it with the `--server.port` option)

## Configuration

You can modify the following constants in `app.py`:

- `API_BASE_URL`: The base URL of the Etherduck API (default: `http://localhost:9988`)
- `CONTRACT_ADDRESS`: The contract address to monitor (default: USDC)
- `EVENT_SIGNATURE`: The event signature hash (default: Transfer event)
- `CONTRACT_NAME`: Display name for the contract (default: "USDC")
- `EVENT_NAME`: Display name for the event (default: "Transfer(address, address, uint256)")

## Dashboard Components

### Events per Hour Graph
Displays a line chart showing the number of Transfer events per hour over the last 7 days. The graph automatically updates every minute.

### Top 5 Addresses Table
Shows the addresses (from topic1) with the most transactions in the last 24 hours. The table is sorted by transaction count in descending order.

### Gauge Indicators
Four gauge-style indicators showing real-time event counts:
- **Last 5 Minutes**: Events in the past 5 minutes
- **Last 30 Minutes**: Events in the past 30 minutes
- **Last 1 Hour**: Events in the past hour
- **Last 24 Hours**: Events in the past day

## Auto-refresh

The dashboard uses Streamlit's caching mechanism which refreshes data every 60 seconds. You can also manually refresh by clicking the "🔄 Refresh Data" button.

## Troubleshooting

- **No data showing**: Make sure the Etherduck API server is running and has indexed events for the USDC contract
- **Connection errors**: Verify that the API server is accessible at the configured `API_BASE_URL`
- **Empty graphs**: The database may not have events for the specified time range. Try adjusting the time ranges or ensure events have been indexed


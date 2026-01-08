<p align="center">
  <img src="assets/quixote-icon.png" alt="Quixote" width="120"/>
</p>

<h1 align="center">Quixote</h1>

<p align="center">
  <img src="https://img.shields.io/github/actions/workflow/status/bilinearlabs/quixote/ci.yaml?style=for-the-badge&logo=github&label=BUILD" alt="Build Status"/>
  <img src="https://img.shields.io/github/license/bilinearlabs/quixote?style=for-the-badge" alt="License"/>
  <a href="https://discord.gg/Et8BTnVBZS"><img src="https://img.shields.io/badge/Discord-Join%20Us-5865F2?style=for-the-badge&logo=discord&logoColor=white" alt="Discord"/></a>
  <img src="https://img.shields.io/badge/Rust-000000?style=for-the-badge&logo=rust&logoColor=white" alt="Rust"/>
  <img src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black" alt="DuckDB"/>
</p>

<p align="center">
  <strong>Blazing-fast blockchain event indexing, powered by Rust and DuckDB</strong>
</p>

<p align="center">
  <em>Index any EVM chain. Query with SQL. Visualize instantly.</em>
</p>

---

## 🚀 Why Quixote?

**Quixote** is a lightweight event indexer for EVM-compatible blockchains. Built in Rust with DuckDB as its storage engine, it focuses on doing one thing well: getting blockchain events into a queryable database as fast as possible.

| Feature | Quixote |
|---------|---------|
| ⚡ **Performance** | Rust-native with async I/O and multiple RPC support |
| 💰 **RPC Cost Control** | Configurable block range to optimize RPC calls and reduce costs |
| 🦆 **Storage** | DuckDB — the fastest analytical database for local queries |
| 🔄 **Resume Support** | Automatically continues from the last synced block |
| 🌐 **Multi-chain** | Works with any EVM-compatible blockchain |
| 📊 **Built-in Dashboard** | Streamlit-powered frontend included out of the box |
| 🔌 **REST API** | Query your indexed data programmatically |
| 📈 **Prometheus Metrics** | Production-ready observability built-in |
| 🎯 **Event Filtering** | Filter events at the source — index only what you need |

---

## ✨ Features at a Glance

### 🎯 Precision Indexing
Index specific events or entire contract ABIs. Filter by address, topic, or any indexed parameter. No more indexing data you don't need.

### 🦆 DuckDB-Powered Analytics
Your indexed events live in a DuckDB database — run complex analytical queries at lightning speed. Export to Parquet, join with other data sources, or power your dashboards.

### 📊 Instant Visualization
Launch `quixote` and immediately access a beautiful Streamlit dashboard at `http://localhost:8501`. No setup required.

### 🔌 Developer-Friendly API
A REST API runs alongside your indexer for programmatic access. List events, query contracts, or execute raw SQL — all via simple HTTP endpoints.

### 🔄 Resilient by Design
Network hiccups? RPC rate limits? Quixote handles it all with intelligent exponential backoff and automatic retry logic. Your indexing job will survive anything.

### 💰 RPC Cost Control
Using a paid RPC provider? The `--block-range` parameter lets you control how many blocks are fetched per request. Tune it to match your provider's limits and pricing — fetch more blocks per call to reduce total requests, or dial it down to stay within rate limits. Every RPC call counts when you're paying per request.

---

## 📦 Quick Start

### One-Liner to Start Indexing

```bash
quixote -r https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -e "Transfer(address indexed from, address indexed to, uint256 amount)" \
    -s 23744000
    --block_range 10
```

That's it! You're now indexing USDT Transfer events on Ethereum mainnet. 🎉

### What Just Happened?

1. **Connected** to an Ethereum RPC endpoint
2. **Started indexing** Transfer events from block 23,744,000
3. **Stored** events in a local DuckDB database
4. **Launched** a REST API at `http://localhost:9720`
5. **Opened** a dashboard at `http://localhost:8501`

---

## 🛠️ Installation

### Pre-built Binaries

Download the latest release for your platform from our [Releases page](https://github.com/bilinearlabs/quixote/releases).

### Build from Source

Building requires linking against DuckDB. Here's how:

```bash
cargo build --release
```

---

## 📖 Usage

### Command Line Interface

```
Usage: quixote [OPTIONS]

Options:
  -r, --rpc-host <RPC_HOST>          RPC URL to index (supports basic auth)
  -c, --contract <CONTRACT>          Contract address to index
  -e, --event <EVENT>                Event signature(s) to index (can be repeated)
  -a, --abi-spec <ABI_SPEC>          Path to ABI JSON (indexes all events)
  -s, --start-block <START_BLOCK>    Starting block number [default: 0]
  -d, --database <DATABASE>          Database file path [default: quixote_indexer.duckdb]
  -j, --api-server <API_SERVER>      API server address [default: 127.0.0.1:9720]
      --block-range <BLOCK_RANGE>    Blocks per RPC request [default: 10000]
  -v, --verbosity <VERBOSITY>        Log level: 0=WARN, 1=INFO, 2=DEBUG, 3=TRACE
      --disable-frontend             Don't launch the dashboard
      --strict-mode                  Stop on first processing error
      --config <CONFIG>              Path to YAML configuration file
  -h, --help                         Print help
  -V, --version                      Print version
```

### Examples

#### Index Multiple Events

```bash
quixote -r https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -e "Transfer(address indexed from, address indexed to, uint256 amount)" \
    -e "Approval(address indexed owner, address indexed spender, uint256 value)" \
    -s 23744000
```

#### Index from ABI

```bash
quixote -r https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -a ./usdt_abi.json \
    -s 23744000
```

#### Using a Configuration File

For advanced use cases like event filtering or multi-job indexing, use a YAML config:

```yaml
database_path: "quixote_indexer.duckdb"
api_server_address: "127.0.0.1"
api_server_port: 9720
frontend_address: "127.0.0.1"
frontend_port: 8501

index_jobs:
  - rpc_url: "https://eth.llamarpc.com"
    contract: "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    events:
      - full_signature: "Transfer(address indexed from, address indexed to, uint256 amount)"
        filters:
          from: "0x12347F958D2ee523a2206206994597C13D831ec7"
      - full_signature: "Approval(address indexed owner, address indexed spender, uint256 value)"

  - rpc_url: "https://polygon-rpc.com"
    contract: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    events:
      - full_signature: "Transfer(address indexed from, address indexed to, uint256 amount)"
```

```bash
quixote --config quixote_config.yaml
```

---

## 🔌 REST API

The built-in REST API makes it easy to query your indexed data programmatically.

| Endpoint | Description |
|----------|-------------|
| `POST /list_events` | List all indexed event types |
| `POST /list_contracts` | List all indexed contracts |
| `POST /get_events` | Query events by contract and time range |
| `POST /raw_query` | Execute raw SQL queries |

### Quick Example

```bash
# List all indexed events
curl -X POST http://localhost:9720/list_events -H "Content-Type: application/json" | jq

# Query events for a specific time range
curl -X POST http://localhost:9720/get_events \
  -H "Content-Type: application/json" \
  -d '{
    "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "start_time": "2024-01-15T00:00:00Z",
    "end_time": "2024-01-16T00:00:00Z"
  }' | jq
```

📚 See [API_EXAMPLES.md](./API_EXAMPLES.md) for comprehensive documentation.

---

## 🗄️ Database Schema

Quixote uses DuckDB with a dynamic schema that adapts to your indexed events.

### Core Tables

| Table | Description |
|-------|-------------|
| `quixote_info` | Metadata about indexed blocks |
| `blocks_<chain_id>` | Record of all indexed blocks with timestamps |
| `event_descriptor` | Registry of indexed event types |
| `event_<chain_id>_<event_name>_<hash_prefix>` | One table per event type |

> 💡 The `hash_prefix` is the first 5 hex characters of the event's keccak256 hash — for example, `ddf25` comes from `0xddf252ad...`

### Query Your Data

```sql
-- Check indexing progress
SELECT * FROM event_descriptor;

-- Count transfers (chain 1 = Ethereum mainnet)
SELECT COUNT(*) FROM event_1_transfer_ddf25;

-- Top receivers
SELECT "to", COUNT(*) as transfers
FROM event_1_transfer_ddf25
GROUP BY "to"
ORDER BY transfers DESC
LIMIT 10;
```

> 💡 DuckDB only allows one process at a time. To query the database directly, stop the indexer first — or use the REST API while indexing.

---

## 📈 Monitoring

Quixote exposes Prometheus metrics for production deployments.

```bash
quixote --metrics --metrics-address 0.0.0.0 --metrics-port 9090 ...
```

---

## 🧪 Testing

```bash
export QUIXOTE_TEST_RPC=<your-rpc-url>
export QUIXOTE_TEST_RPC_USER=<optional-user>
export QUIXOTE_TEST_RPC_PASSWORD=<optional-password>
cargo test
```

> 💡 Some tests require a connection to an RPC.

---

## 🤝 Contributing

We welcome contributions! Whether it's bug reports, feature requests, or pull requests — we'd love to hear from you.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🏢 About Bilinear Labs

**Quixote** is built and maintained by [Bilinear Labs](https://www.bilinearlabs.io), a team passionate about building high-performance infrastructure for the decentralized web.

We specialize in:
- 🔗 **Blockchain Infrastructure** — Indexers, RPCs, and data pipelines
- 🦀 **Rust Development** — Performance-critical systems and tooling
- 📊 **Data Engineering** — Real-time analytics and event processing

**Interested in working together?** [Get in touch](https://www.bilinearlabs.io) or [join our Discord](https://discord.gg/Et8BTnVBZS)!

---

<p align="center">
  <strong>Built with ❤️ by <a href="https://www.bilinearlabs.io">Bilinear Labs</a></strong>
</p>

<p align="center">
  <a href="https://github.com/bilinearlabs/quixote">⭐ Star us on GitHub</a> •
  <a href="https://discord.gg/Et8BTnVBZS">💬 Join our Discord</a> •
  <a href="https://www.bilinearlabs.io">🌐 Visit our website</a>
</p>

<p align="center">
  <sub>Icon by <a href="https://www.flaticon.com/free-icons/don-quixote" title="don quixote icons">Eucalyp - Flaticon</a></sub>
</p>

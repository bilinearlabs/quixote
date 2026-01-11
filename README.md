<p align="center">
  <img src="assets/quixote-icon.png" alt="quixote" width="120"/>
</p>

<p align="center">
  <strong>From the blockchain to your database. Index RWAs, Stablecoins, and Digital Assets.</strong>
</p>

# quixote

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/bilinearlabs/quixote/ci.yaml?style=flat-square)
![GitHub License](https://img.shields.io/github/license/bilinearlabs/quixote?style=flat-square)
![Rust](https://img.shields.io/badge/Rust-000000?style=flat-square&logo=rust&logoColor=white)
![Duckdb](https://img.shields.io/badge/DuckDB-FFF000?style=flat-square&logo=duckdb&logoColor=black)
[![Join our Discord](https://img.shields.io/badge/Discord-5865F2?logo=discord&logoColor=white&style=flat-square)](https://discord.gg/Et8BTnVBZS)


**quixote** is a lightweight, high-performance EVM event indexer built in Rust. It lets you capture, store, and query blockchain events with minimal setup. Get it up and running in two commands.

Index and query on-chain data from stablecoins, RWAs, DeFi protocols, or any asset on EVM-compatible blockchains.

Just point it at an RPC, specify the events you care about, and start querying your data with SQL.

---

## Quickstart

Install:

```bash
curl -fsSL https://quixote.bilinearlabs.io/install | bash -s
```

Index all existing pools in Uniswap V4:

```bash
quixote -r https://gateway.tenderly.co/public/mainnet \
  -c 0x000000000004444c5dc75cB358380D2e3dE08A90 \
  -e "Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)" \
  -s 21688329
```

Now `quixote` will start indexing the `Initialize` event for this smart contract starting from block `21688329` and dumping all the content into `quixote_indexer.duckdb`. You can check the sync status as follows.

```bash
curl http://localhost:9720/list_events
```

And the database schema as follows.

```bash
curl http://localhost:9720/db_schema
```

You can also pass raw SQL queries as follows. 

```bash
curl -X POST http://localhost:9720/raw_query \
-H "Content-Type: application/json" \
-d '{"query": "SELECT * FROM event_1_initialize_dd466 limit 5"}'
```

And `quixote` ships with a built-in frontend that you can use to query data in a simple way, showing the content as a table. Go to `http://127.0.0.1:8501`




## Features

- **Simple to run**: Single binary, minimal configuration. Point to an RPC and start indexing.
- **DuckDB-powered**: Fast analytical queries with SQL. File-based storage, no external services required. Export to Parquet or join with other data sources.
- **Any EVM chain**: Works with Ethereum, Arbitrum, Optimism, Polygon, and any EVM-compatible chain.
- **Flexible event selection**: Index specific events or entire ABIs. Filter by address, topic, or any indexed parameter.
- **Built-in REST API**: List events, query contracts, or execute raw SQL via HTTP endpoints.
- **Built-in frontend**: Embedded Streamlit dashboard at `http://localhost:8501` for data exploration.
- **Auto resume**: Automatically resumes from the last synced block.
- **Resilient**: Exponential backoff and retry logic for network issues and RPC rate limits.
- **RPC cost control**: The `--block-range` parameter controls blocks per request to match provider limits.
- **YAML configuration**: Advanced filtering and multi-job support via config files.
- **Built in Rust**: Fast, safe, and memory-efficient.


## License

This project is open source and available under the [MIT License](LICENSE).


## About Bilinear Labs

`quixote` is built and maintained by [Bilinear Labs](https://www.bilinearlabs.io), a team passionate about building high-performance infrastructure for blockchain and finance.

Need help with custom indexing solutions, high-performance backends, or managed infrastructure?
We're happy to help. Whether it's tailoring quixote for your specific use case, building something bespoke, or running the infrastructure so you don't have to. Feel free to reach out.
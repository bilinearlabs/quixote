# Rust-based Indexing Tool That Indexes EVM Events

Simple command line tool that allows indexing events of a blockchain.

## Supported Features

- Storage of the indexed data in a data base.
- Support for DuckDB as main storage.
- Support for indexing ERC20 Transfer events.


## How To Use The Tool

The tool expects a set of arguments that identify the RPC host from which the data is going to be pulled, along the target event for the indexing and the contract address.

```raw
Quixote

Usage: quixote [OPTIONS] --rpc-host <RPC_HOST> --contract <CONTRACT>

Options:
  -r, --rpc-host <RPC_HOST>        RPC host to index.

                                   Format: <chain_id>[:<username>:<password>]@<host>:<port>

                                   Example for an RPC with basic auth => 1:user:pass@http://localhost:9822

                                   Example for an authless RPC => 1@http://localhost:9822
  -c, --contract <CONTRACT>        Contract to index.
                                   Example: 0x1234567890123456789012345678901234567890
  -e, --event <EVENT>              Event to index as defined by the contract's ABI.
                                   Example: Transfer(address indexed from, address indexed to, uint256 amount)
  -s, --start-block <START_BLOCK>  Start block to index (decimal). If the database is not empty, the indexer will resume from the last synchronized block, thus the given start block would be ignored.
                                   Example => 28837711
                                   Default: latest
  -d, --database <DATABASE>        Path to the database file. Default: quixote_indexer.duckdb
  -a, --abi-spec <ABI_SPEC>        Path for the ABI JSON spec of the indexed contract. When give, the entire set of events defined in the ABI will be indexed.
  -j, --api-server <API_SERVER>    Interface and port in which the API server will listen for requests. Defaults to 127.0.0.1:9720
  -h, --help                       Print help (see more with '--help')
  -V, --version                    Print version
```

This way, an example of usage would be:

```bash
$ quixote -r 1@https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -e "Transfer(address indexed from, address indexed to, uint256 amount)" \
    -s 23744000
```

To increase the verbosity level up to _debug_, the environment variable `RUST_LOG` shall be used:

```bash
$ quixote -r 1@https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -e "Transfer(address indexed from, address indexed to, uint256 amount)" \
    -s 23744000
```

Another example for indexing Transfer and Approval events:

```bash
$ quixote -r 1@https://eth.llamarpc.com \
    -c 0xdAC17F958D2ee523a2206206994597C13D831ec7 \
    -e "Transfer(address indexed from, address indexed to, uint256 amount)" \
    -e "Approval(address indexed owner, address indexed spender, uint256 value)" \
    -s 23744000
```

The previous call would launch an instance of the tool for indexing ERC20 Transfer events in the Ethereum Mainnet blockchain for the Tether USD smartcontract, starting from the block $23.744.000$. It will use an RPC from [chainlist](https://chainlist.org). Though using an RPC from the **chainlist** is not advised for long-term indexing.

The event needs to be fully defined to properly index the events for the chosen smartcontract. **Take the definition from the contract's ABI.**.

### Embedded Frontend

The indexer launches a frontend along the main indexer service, which is available at `http://127.0.0.1:8501`.

## Data Base Schema

The data base includes this schema:

### Table *blocks*

This table is a record of the indexed blocks. The first and latest indexed blocks can be easily retrieved from a metadata table, though:

```sql
SELECT first_block, last_block FROM quixote_info;
```

The table *blocks* includes these fields:
- *block_number* (String) (PK)
- *block_hash* (Unsigned)
- *block_timestamp* (Unsigned)

### Table *event_descriptor*

This table is a record of the indexed events by type. This table shall be used to properly parse the content from each event entry from the *event_X* tables. Includes these fields:

- *event_hash* (String) (PK)
- *event_signature* (String)
- *event_name* (String)

### Tables *event_<hash>*

Each indexed event makes use of a table in the DB named using the event's hash of its full signature. For instance, for the event `Transfer(address indexed from, address indexed to, uint256 amount)` the table in the DB would be named *event_0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef*.

These tables are dynamic, which means there's no fix schema, and it is rather based on the event's content. Usually, a table of this type includes:

- *block_number* (String) (PK)
- *transaction_hash* (String) (PK)
- *log_index* (Unsiged) (PK)
- *contract_address* (String)

Aside from that, an event might include up to 3 indexed topics (fields *topic_{0,1,2}*) and several non-indexed topics.

# Indexing Modes

The tool distinguish two modes of operation:

- The **ABI** mode, in which the user specifies a JSON file with the definition of the ABI of some smart contract.
- The **event** mode, in which the user specifies one or many single events using their canonical definition.

Both modes are excluding: the **event** mode takes precedence over the **ABI** mode when both `-a` and `-e` are used in the command line interface.

# Resuming a Previous Indexing

The indexer saves the synchronisation status for each indexed event in the DB. This way, if the indexer stops for any reason, it can resume indexing from the last synchronised block. There's a limitation, though. If the DB contains data from a previous **event** operation mode, it won't be able to resume indexing in **ABI** mode.

Consider an initial indexing of 2 events of a smart contract's ABI. Later, you decide that you'd rather like to index the whole contract. This scenario is not supported, as the **ABI** mode requires all the included events to synchronize up to the same block. This check is not ensured when running in **event** mode, as each indexing task is launched asynchronously, and potentially to several RPC servers, which might ultimately end in having one indexing task going faster than the other.

# Development

## Building the Project

Building the project requires dynamic linking to DuckDB to avoid compiling it from source. Follow these steps (adjust for your architecture):
- Linux:
```bash
$ wget https://github.com/duckdb/duckdb/releases/download/v1.4.2/libduckdb-linux-amd64.zip \
$ unzip libduckdb-linux-amd64.zip -d libduckdb

$ export DUCKDB_LIB_DIR=$PWD/libduckdb
$ export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
$ export LD_LIBRARY_PATH=$DUCKDB_LIB_DIR

$ cargo build
```
- macOS:
```bash
$ wget wget https://github.com/duckdb/duckdb/releases/download/v1.4.2/libduckdb-osx-universal.zip
$ unzip libduckdb-osx-universal.zip -d libduckdb

$ export DUCKDB_LIB_DIR=$PWD/libduckdb
$ export DUCKDB_INCLUDE_DIR=$DUCKDB_LIB_DIR
$ export LD_LIBRARY_PATH=$DUCKDB_LIB_DIR

$ cargo build
```

## Testing

In order to run some of the included tests, a connection to a powerful RPC server is required. The connection is handled via environment variables. You'll need to populate the following variables before running the tests:

```bash
$ export QUIXOTE_TEST_RPC=<http://myserver.com:8765>
$ export QUIXOTE_TEST_RPC_USER=<user>
$ export QUIXOTE_TEST_RPC_PASSWORD=<password>
```

Then simply run `cargo test`.

## How To Package the Frontend

The included frontend runs as a Python application that is called within the main indexer service. To ease running the embedded Python script, a complete environment that includes the Python interpreter along all the needed dependencies is delivered. This way, users of the app won't need to struggle with their local Python installation nor dependencies.

The only requirement to build the environment package is [Conda](https://docs.conda.io/en/latest/). Having **Conda** installed using any of its flavours, the following steps will generate the package to run the frontend:

```bash
$ conda create -n quixote_frontend_env python=3.11 streamlit pyarrow -y
$ conda install -c conda-forge conda-pack -y
$ conda pack -n quixote_frontend_env -o quixote_frontend_env.tar.gz
```

The compressed package can be distributed so end users only need to extract it to run the full application.

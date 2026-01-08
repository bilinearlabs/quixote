// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the command line interface.

use crate::constants;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author = "Bilinear Labs")]
#[command(version = "0.1.0")]
#[command(about = "Quixote")]
#[command(long_about = "EVM-compatible event indexing tool")]
pub struct IndexingArgs {
    #[arg(
        short,
        long,
        required_unless_present = "config",
        help = "RPC URL to index.
            \nFormat: <scheme>://[<username>:<password>@]<host>[:<port>]
            \nExample for an RPC with basic auth => http://user:pass@localhost:8545
            \nExample for an authless RPC => http://localhost:8545"
    )]
    pub rpc_host: Option<String>,
    #[arg(
        short,
        long,
        required_unless_present = "config",
        help = "Contract to index.\nExample: 0x1234567890123456789012345678901234567890"
    )]
    pub contract: Option<String>,
    #[arg(
        short,
        long,
        help = "Event to index as defined by the contract's ABI.\nExample: Transfer(address indexed from, address indexed to, uint256 amount)"
    )]
    pub event: Option<Vec<String>>,
    #[arg(
        short,
        long,
        help = "Start block to index (decimal). If the database is not empty, the indexer will resume from the last synchronized block, thus the given start block would be ignored.\nExample => 28837711\nDefault: 0"
    )]
    pub start_block: Option<String>,
    #[arg(
        short,
        long,
        help = "Path to the database file. Default: etherduck_indexer.duckdb"
    )]
    pub database: Option<String>,
    #[arg(
        short,
        long,
        help = "Path for the ABI JSON spec of the indexed contract. When give, the entire set of events defined in the ABI will be indexed."
    )]
    pub abi_spec: Option<String>,
    #[arg(
        short = 'j',
        long,
        help = "Interface and port in which the API server will listen for requests. Defaults to 127.0.0.1:9720"
    )]
    pub api_server: Option<String>,
    #[arg(
        long,
        help = "Block range for the RPC requests. Applies to all collectors.",
        default_value_t = constants::DEFAULT_BLOCK_RANGE
    )]
    pub block_range: usize,
    #[arg(
        short,
        long,
        help = "Verbosity level. 0 = WARN, 1 = INFO (default), 2 = DEBUG, 3 = TRACE",
        default_value_t = 1
    )]
    pub verbosity: u8,
    #[arg(
        long,
        help = "Disable the frontend application.",
        default_value_t = false
    )]
    pub disable_frontend: bool,
    #[arg(
        long,
        help = "Frontend listening address",
        default_value_t = String::from("127.0.0.1"),
    )]
    pub frontend_address: String,
    #[arg(long, help = "Frontend listening port", default_value_t = 8501)]
    pub frontend_port: u16,
    #[arg(
        long,
        help = "Enable strict mode (stops the indexing when an event fails to be processed)",
        default_value_t = false
    )]
    pub strict_mode: bool,
    #[arg(
        long,
        help = "Path to the configuration file. When used, the command line arguments will be ignored."
    )]
    pub config: Option<String>,
    #[arg(
        long,
        help = "Enable the Prometheus metrics endpoint.",
        default_value_t = false
    )]
    pub metrics: bool,
    #[arg(
        long,
        help = "Metrics server listening address",
        default_value_t = String::from("127.0.0.1"),
    )]
    pub metrics_address: String,
    #[arg(long, help = "Metrics server listening port", default_value_t = 5054)]
    pub metrics_port: u16,
    #[arg(
        long,
        help = "Optional Access-Control-Allow-Origin value for the metrics endpoint"
    )]
    pub metrics_allow_origin: Option<String>,
}

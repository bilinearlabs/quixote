// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the command line interface.

use crate::constants;
use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
#[command(author = "Bilinear Labs")]
#[command(version = "0.1.0")]
#[command(about = "Quixote")]
#[command(long_about = "Ethereum event indexing tool")]
pub struct IndexingArgs {
    #[arg(
        short,
        long,
        help = "RPC host to index.
            \nFormat: <chain_id>[:<username>:<password>]@<host>:<port>
            \nExample for an RPC with basic auth => 1:user:pass@http://localhost:9822
            \nExample for an authless RPC => 1@http://localhost:9822"
    )]
    pub rpc_host: String,
    #[arg(
        short,
        long,
        help = "Contract to index (optional).\n\
            Example: 0x1234567890123456789012345678901234567890\n\
            When omitted, all contracts emitting the selected events will be indexed."
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
        long,
        help = "Indexed topic filters in the form EventName.field=value (repeatable).\n\
            The field must be indexed in the event. Example:\n\
            --filter \"Transfer.from=0x396343362be2a4da1ce0c1c210945346fb82aa49\""
    )]
    pub filter: Option<Vec<String>>,
    #[arg(
        long,
        value_enum,
        default_value_t = FilterMode::And,
        help = "How to combine filters across indexed params: and (default) or"
    )]
    pub filter_mode: FilterMode,
    #[arg(
        short = 'j',
        long,
        help = "Interface and port in which the API server will listen for requests. Defaults to 127.0.0.1:9720"
    )]
    pub api_server: Option<String>,
    #[arg(
        long,
        help = "Block range for the RPC requests.",
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
}

#[derive(ValueEnum, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FilterMode {
    #[default]
    And,
    Or,
}

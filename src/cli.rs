// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the command line interface.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author = "Bilinear Labs")]
#[command(version = "0.1.0")]
#[command(about = "Etherduck")]
#[command(long_about = "Ethereum event indexing tool")]
pub(crate) struct IndexingArgs {
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
        help = "Contract to index.\nExample: 0x1234567890123456789012345678901234567890"
    )]
    pub contract: String,
    #[arg(
        short,
        long,
        help = "Event to index as defined by the contract's ABI.\nExample: Transfer(address indexed from, address indexed to, uint256 amount)"
    )]
    pub event: String,
    #[arg(
        short,
        long,
        help = "Start block to index (decimal). If the database is not empty, the indexer will resume from the last synchronized block, thus the given start block would be ignored.\nExample => 28837711\nDefault: latest"
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
}

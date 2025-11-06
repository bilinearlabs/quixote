// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use clap::Parser;
use anyhow::Result;

#[derive(Parser, Debug)]
#[command(author = "Bilinear Labs")]
#[command(version = "0.1.0")]
#[command(about = "Etherduck")]
#[command(long_about = "Ethereum event indexing tool")]
struct Args {
    #[arg(
        short,
        long,
        help = "RPC hosts to index.\nExample: http://localhost:8545[, http://localhost:8546, ...]"
    )]
    rpc_hosts: Vec<String>,
    #[arg(
        short,
        long,
        help = "Contract to index.\nExample: 0x1234567890123456789012345678901234567890"
    )]
    contract: String,
    #[arg(
        short,
        long,
        help = "Events to index.\nExample: Transfer(address,address), Approval(address,address)"
    )]
    events: Vec<String>,
    #[arg(short, long, help="Start block to index.\nExample: 0x1000000\nDefault: latest")]
    start_block: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Args: {:?}", args);
    parse_arguments(args)?;

    Ok(())
}

fn parse_arguments(_args: Args) -> Result<()> {
    todo!()
}

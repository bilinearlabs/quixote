// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::ProviderBuilder,
    rpc::client::RpcClient,
    transports::{
        TransportError,
        layers::{RetryBackoffLayer, RetryPolicy},
    },
};
use anyhow::Result;
use clap::Parser;
use etherduck::{DuckDBStorage, RpcHost};
use etherduck::{EventCollector, EventProcessor};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(author = "Bilinear Labs")]
#[command(version = "0.1.0")]
#[command(about = "Etherduck")]
#[command(long_about = "Ethereum event indexing tool")]
struct Args {
    #[arg(
        short,
        long,
        help = "RPC hosts to index.
            \nFormat: <chain_id>[:<username>:<password>]@<host>:<port>[,<chain_id>[:<username>:<password>@]<host>:<port>, ...]
            \nExample for an RPC with basic auth => 1:user:pass@http://localhost:9822
            \nExample for an authless RPC => 1@http://localhost:9822"
    )]
    rpc_hosts: Vec<String>,
    #[arg(
        short,
        long,
        help = "Contract to index.\nExample => 0x1234567890123456789012345678901234567890"
    )]
    contract: String,
    #[arg(
        short,
        long,
        help = "Events to index.\nExample => Transfer(address,address), Approval(address,address)"
    )]
    events: Vec<String>,
    #[arg(
        short,
        long,
        help = "Start block to index (decimal).\nExample => 28837711\nDefault: latest"
    )]
    start_block: Option<String>,
    #[arg(
        short,
        long,
        help = "Path to the database file. Default: etherduck_indexer.duckdb"
    )]
    database: Option<String>,
}

#[derive(Debug, Copy, Clone, Default)]
#[non_exhaustive]
pub struct AlwaysRetryPolicy;

impl RetryPolicy for AlwaysRetryPolicy {
    fn should_retry(&self, _error: &TransportError) -> bool {
        // TODO: Be more granular with the retry policy.
        // we don't want to retry in some cases.
        true
    }

    fn backoff_hint(&self, _error: &TransportError) -> Option<std::time::Duration> {
        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (rpc_hosts, contract_address, events, start_block) = parse_arguments(&args)?;

    println!("RPC hosts: {:?}", rpc_hosts);
    println!("Contract address: {:?}", contract_address);
    println!("Events: {:?}", events);
    println!("Start block: {:?}", start_block);

    // TODO: support multiple RPC hosts
    let rpc_host = rpc_hosts.iter().take(1).next().unwrap();
    let max_retry: u32 = 100;
    let backoff: u64 = 2000;
    let cups: u64 = 100;
    let always_retry_policy = AlwaysRetryPolicy::default();

    let retry_policy =
        RetryBackoffLayer::new_with_policy(max_retry, backoff, cups, always_retry_policy);

    let provider = Arc::new(
        ProviderBuilder::new().connect_client(
            RpcClient::builder()
                .layer(retry_policy)
                .http(rpc_host.try_into()?),
        ),
    );

    let (producer_buffer, consumer_buffer) = mpsc::channel(100);

    let event_collector = EventCollector::new(
        &contract_address,
        &events,
        &start_block,
        &BlockNumberOrTag::Latest,
        provider,
        producer_buffer,
    );

    let storage = if let Some(db_path) = &args.database {
        DuckDBStorage::with_db(&db_path)?
    } else {
        DuckDBStorage::new()?
    };
    storage.include_events(&events)?;

    let mut event_processor = EventProcessor::new(Arc::new(storage), consumer_buffer);
    event_collector.collect().await?;

    let handle = tokio::spawn(async move {
        event_processor.process().await.unwrap();
    });

    handle.await?;

    Ok(())
}

fn parse_arguments(args: &Args) -> Result<(Vec<RpcHost>, Address, Vec<String>, BlockNumberOrTag)> {
    let rpc_hosts = args
        .rpc_hosts
        .iter()
        .map(|host| host.parse::<RpcHost>())
        .collect::<Result<Vec<RpcHost>>>()?;

    println!("RPC hosts: {:?}", rpc_hosts);

    let contract_address = args.contract.parse::<Address>()?;

    println!("Contract address: {:?}", contract_address);

    let events = args.events.clone();

    let start_block = if let Some(block) = &args.start_block {
        BlockNumberOrTag::Number(block.parse::<u64>()?)
    } else {
        BlockNumberOrTag::Latest
    };

    Ok((rpc_hosts, contract_address, events, start_block))
}

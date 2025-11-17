// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use alloy::{eips::BlockNumberOrTag, primitives::Address};
use anyhow::Result;
use clap::Parser;
use etherduck::{
    CancellationToken, DuckDBStorage, EventCollectorRunner, EventProcessor, RpcHost, Storage,
    api_rest::start_api_server,
};
use std::sync::Arc;
use tokio::{
    signal::ctrl_c,
    sync::{Mutex, mpsc},
};

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
    #[arg(
        short,
        long,
        help = "Path for the ABI JSON spec of the indexed contract."
    )]
    abi_spec: Option<String>,
    #[arg(
        short = 'j',
        long,
        help = "Interface and port in which the API server will listen for requests. Defaults to 127.0.0.1:9720"
    )]
    api_server: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (rpc_hosts, contract_address, events, start_block) = parse_arguments(&args)?;

    println!("RPC hosts: {:?}", rpc_hosts);
    println!("Contract address: {:?}", contract_address);
    println!("Events: {:?}", events);
    println!("Start block: {:?}", start_block);
    println!("ABI spec: {:?}", args.abi_spec);

    let not_indexed_params = if let Some(abi_spec) = &args.abi_spec {
        let abi_spec = std::fs::read_to_string(abi_spec)?;
        let abi_parser =
            etherduck::AbiParser::new(serde_json::from_str::<serde_json::Value>(&abi_spec)?);
        let event_parts: Vec<String> = events[0].split('(').map(ToOwned::to_owned).collect();
        let event_name: &str = event_parts.first().map(|s| s.as_str()).unwrap_or("");
        println!("Event name: {:?}", event_name);
        let not_indexed_params = abi_parser.find_event_not_indexed_params(event_name)?;
        println!("Not indexed params: {:?}", not_indexed_params);
        not_indexed_params
    } else {
        None
    };

    let (producer_buffer, consumer_buffer) = mpsc::channel(100);

    let cancellation_token = CancellationToken::new();

    let storage = if let Some(db_path) = &args.database {
        DuckDBStorage::with_db(&db_path, not_indexed_params.is_some())?
    } else {
        DuckDBStorage::new(not_indexed_params.is_some())?
    };
    let storage = Arc::new(storage);
    storage.include_events(&events, not_indexed_params.clone())?;
    // Clone the inner DuckDBStorage to create a concurrent new connection for the API server.
    let storage_for_api = Arc::new((*storage).clone());
    storage_for_api.include_events(&events, not_indexed_params)?;

    let last_block = storage.last_block()?;
    let first_block = storage.first_block()?;
    println!("Last block: {:?}", last_block);
    println!("First block: {:?}", first_block);

    // Select the target block based on the input and the current DB status.
    let target_block = choose_target_block(&storage, start_block)?;

    let event_collector_runner = EventCollectorRunner::new(
        rpc_hosts.as_slice(),
        contract_address,
        events,
        target_block,
        producer_buffer,
    )?;

    let mut event_processor = EventProcessor::new(
        storage,
        target_block.as_number().unwrap(),
        consumer_buffer,
        cancellation_token.clone(),
    );

    let api_server_address = args.api_server.unwrap_or("127.0.0.1:9720".to_string());

    // Start the REST API server
    start_api_server(
        api_server_address.as_str(),
        storage_for_api,
        cancellation_token.clone(),
    )?;

    // Spawn both tasks concurrently
    let collector_handle = tokio::spawn(async move { event_collector_runner.run().await });

    let processor_handle = tokio::spawn(async move { event_processor.process().await });

    // Wrap handles in Arc<Mutex<>> so we can abort them from Ctrl+C handler
    let collector_handle_arc = Arc::new(Mutex::new(Some(collector_handle)));
    let processor_handle_arc = Arc::new(Mutex::new(Some(processor_handle)));
    let collector_handle_for_ctrl_c = collector_handle_arc.clone();
    let cancellation_token_for_ctrl_c = cancellation_token.clone();

    // Spawn a task that handles Ctrl+C and aborts the collector
    let ctrl_c_task = tokio::spawn(async move {
        ctrl_c().await.ok();
        println!("\nReceived Ctrl+C, shutting down gracefully...");
        // Abort the collector task immediately - this will stop all its child tasks
        if let Some(handle) = collector_handle_for_ctrl_c.lock().await.take() {
            handle.abort();
        }
        // Signal cancellation to processor
        cancellation_token_for_ctrl_c.graceful_shutdown();
    });

    // Wait for either Ctrl+C task or both tasks to complete
    tokio::select! {
        _ = ctrl_c_task => {
            // Ctrl+C was received, tasks are being aborted
            // Wait for all tasks to finish
            let collector_handle = collector_handle_arc.lock().await.take();
            let processor_handle = processor_handle_arc.lock().await.take();
            let (collector_result, processor_result) = tokio::join!(
                async {
                    if let Some(handle) = collector_handle {
                        handle.await
                    } else {
                        Ok(Err(anyhow::anyhow!("Collector was aborted")))
                    }
                },
                async {
                    if let Some(handle) = processor_handle {
                        handle.await
                    } else {
                        Ok(Err(anyhow::anyhow!("Processor was aborted")))
                    }
                },
            );

            match collector_result {
                Ok(_) | Err(_) => {
                    // Collector was aborted or completed
                }
            }

            match processor_result {
                Ok(Ok(())) => {
                    println!("Event processor stopped gracefully");
                }
                Ok(Err(e)) => {
                    eprintln!("Event processor error during shutdown: {}", e);
                }
                Err(e) => {
                    eprintln!("Event processor task panicked during shutdown: {}", e);
                }
            }

            println!("Shutdown complete");
        }
        _ = async {
            // Wait for all tasks to complete
            let collector_handle = collector_handle_arc.lock().await.take();
            let processor_handle = processor_handle_arc.lock().await.take();
            let (collector_result, processor_result) = tokio::join!(
                async {
                    if let Some(handle) = collector_handle {
                        handle.await
                    } else {
                        Ok(Err(anyhow::anyhow!("Collector was aborted")))
                    }
                },
                async {
                    if let Some(handle) = processor_handle {
                        handle.await
                    } else {
                        Ok(Err(anyhow::anyhow!("Processor was aborted")))
                    }
                },
            );

            match collector_result {
                Ok(Ok(())) => {
                    println!("Event collector completed");
                }
                Ok(Err(e)) => {
                    eprintln!("Event collector error: {}", e);
                }
                Err(e) => {
                    eprintln!("Event collector task panicked: {}", e);
                }
            }

            match processor_result {
                Ok(Ok(())) => {
                    println!("Event processor completed");
                }
                Ok(Err(e)) => {
                    eprintln!("Event processor error: {}", e);
                }
                Err(e) => {
                    eprintln!("Event processor task panicked: {}", e);
                }
            }

        } => {}
    }

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
        if let Ok(block_num) = block.parse::<u64>() {
            BlockNumberOrTag::Number(block_num)
        } else {
            BlockNumberOrTag::Latest
        }
    } else {
        BlockNumberOrTag::Latest
    };

    Ok((rpc_hosts, contract_address, events, start_block))
}

/// Chooses the starting block based on the input and the current DB status.
fn choose_target_block(
    db_conn: &DuckDBStorage,
    start_block: BlockNumberOrTag,
) -> Result<BlockNumberOrTag> {
    // The DB's first and last synchronized blocks.
    let db_start_block = db_conn.first_block()?;
    let db_last_block = db_conn.last_block()?;

    // Starting block selection logic:
    // 1. If the input is older than the DB's start block, backfill from there.
    // 2. If the input is newer than the DB's last block, continue from the latest synchronized block in the DB.
    // 3. Otherwise, continue from the latest block in the DB.
    match start_block {
        BlockNumberOrTag::Number(n) => {
            // Initial DB state, simply sync from the user's choice
            if db_start_block == 0 && db_start_block == db_last_block {
                println!("Database is empty. Starting from the start block: {n}");
                Ok(BlockNumberOrTag::Number(n))
            } else {
                println!("Continuing from the latest block: {db_last_block}");
                Ok(BlockNumberOrTag::Number(db_last_block + 1))
            }
        }
        // Continue where the DB left off.
        _ => {
            println!("Continuing from the latest block: {db_last_block}");
            Ok(BlockNumberOrTag::Number(db_last_block + 1))
        }
    }
}

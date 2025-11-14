// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use alloy::{eips::BlockNumberOrTag, primitives::Address};
use anyhow::Result;
use clap::Parser;
use etherduck::{
    CancellationToken, DuckDBStorage, EventCollectorRunner, EventProcessor, RpcHost, Storage,
    api_rest::create_router,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let (rpc_hosts, contract_address, events, start_block) = parse_arguments(&args)?;

    println!("RPC hosts: {:?}", rpc_hosts);
    println!("Contract address: {:?}", contract_address);
    println!("Events: {:?}", events);
    println!("Start block: {:?}", start_block);

    let (producer_buffer, consumer_buffer) = mpsc::channel(100);

    let cancellation_token = CancellationToken::new();

    let storage = if let Some(db_path) = &args.database {
        DuckDBStorage::with_db(&db_path)?
    } else {
        DuckDBStorage::new()?
    };
    let storage = Arc::new(storage);
    storage.include_events(&events)?;

    // let storage_for_api = if let Some(db_path) = &args.database {
    //     DuckDBStorage::with_db(&db_path)?
    // } else {
    //     DuckDBStorage::new()?
    // };
    let storage_for_api = storage.clone();

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

    // Start the REST API server
    let api_handle = tokio::spawn(async move {
        let app = create_router(storage_for_api);

        let listener = tokio::net::TcpListener::bind("0.0.0.0:9988")
            .await
            .expect("Failed to bind to port 9988");
        println!("REST API server listening on http://0.0.0.0:9988");
        axum::serve(listener, app).await.expect("API server error");
    });

    // Spawn both tasks concurrently
    let collector_handle = tokio::spawn(async move { event_collector_runner.run().await });

    let processor_handle = tokio::spawn(async move { event_processor.process().await });

    // Wrap handles in Arc<Mutex<>> so we can abort them from Ctrl+C handler
    let collector_handle_arc = Arc::new(Mutex::new(Some(collector_handle)));
    let processor_handle_arc = Arc::new(Mutex::new(Some(processor_handle)));
    let api_handle_arc = Arc::new(Mutex::new(Some(api_handle)));
    let collector_handle_for_ctrl_c = collector_handle_arc.clone();
    let cancellation_token_for_ctrl_c = cancellation_token.clone();

    // Spawn a task that handles Ctrl+C and aborts the collector
    let api_handle_for_ctrl_c = api_handle_arc.clone();
    let ctrl_c_task = tokio::spawn(async move {
        ctrl_c().await.ok();
        println!("\nReceived Ctrl+C, shutting down gracefully...");
        // Abort the collector task immediately - this will stop all its child tasks
        if let Some(handle) = collector_handle_for_ctrl_c.lock().await.take() {
            handle.abort();
        }
        // Signal cancellation to processor
        cancellation_token_for_ctrl_c.graceful_shutdown();
        // Abort the API server
        if let Some(handle) = api_handle_for_ctrl_c.lock().await.take() {
            handle.abort();
        }
    });

    // Wait for either Ctrl+C task or both tasks to complete
    tokio::select! {
        _ = ctrl_c_task => {
            // Ctrl+C was received, tasks are being aborted
            // Wait for all tasks to finish
            let collector_handle = collector_handle_arc.lock().await.take();
            let processor_handle = processor_handle_arc.lock().await.take();
            let api_handle = api_handle_arc.lock().await.take();
            let (collector_result, processor_result, api_result) = tokio::join!(
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
                async {
                    if let Some(handle) = api_handle {
                        handle.await
                    } else {
                        Ok(())
                    }
                }
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

            match api_result {
                Ok(_) => {
                    println!("API server stopped");
                }
                Err(e) => {
                    eprintln!("API server error during shutdown: {}", e);
                }
            }

            println!("Shutdown complete");
        }
        _ = async {
            // Wait for all tasks to complete
            let collector_handle = collector_handle_arc.lock().await.take();
            let processor_handle = processor_handle_arc.lock().await.take();
            let api_handle = api_handle_arc.lock().await.take();
            let (collector_result, processor_result, api_result) = tokio::join!(
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
                async {
                    if let Some(handle) = api_handle {
                        handle.await
                    } else {
                        Ok(())
                    }
                }
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

            match api_result {
                Ok(_) => {
                    println!("API server completed");
                }
                Err(e) => {
                    eprintln!("API server error: {}", e);
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
                if n < db_start_block {
                    println!("Backfilling from the start block: {db_start_block}");
                    Ok(BlockNumberOrTag::Number(db_start_block))
                } else {
                    println!("Continuing from the latest block: {db_last_block}");
                    Ok(BlockNumberOrTag::Number(db_last_block + 1))
                }
            }
        }
        // Continue where the DB left off.
        _ => {
            println!("Continuing from the latest block: {db_last_block}");
            Ok(BlockNumberOrTag::Number(db_last_block + 1))
        }
    }
}

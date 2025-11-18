// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    AbiParser, CancellationToken, DuckDBStorage, EventCollectorRunner, EventProcessor, RpcHost,
    Storage, StorageQuery, api_rest::start_api_server, cli::IndexingArgs, constants,
};
use alloy::{eips::BlockNumberOrTag, primitives::Address};
use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;
use tokio::{join, signal::ctrl_c, sync::mpsc};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub host_list: Vec<RpcHost>,
    pub api_server_address: String,
    pub storage_for_api: Arc<dyn StorageQuery + Send + Sync>,
    pub cancellation_token: CancellationToken,
    pub events: Vec<String>,
    pub not_indexed_params: Option<Vec<String>>,
    pub start_block: BlockNumberOrTag,
    pub contract_address: Address,
}

impl IndexingApp {
    /// Builds a new instance fo the indexing app using the command line arguments.
    pub fn build_app() -> Result<Self> {
        // Parse the command line arguments.
        let args = IndexingArgs::parse();

        let not_indexed_params = if let Some(abi_spec) = &args.abi_spec {
            let abi_spec = std::fs::read_to_string(abi_spec)?;
            let abi_parser = AbiParser::new(serde_json::from_str::<serde_json::Value>(&abi_spec)?);
            let event_parts: Vec<String> = args.event.split('(').map(ToOwned::to_owned).collect();
            let event_name: &str = event_parts.first().map(|s| s.as_str()).unwrap_or("");
            println!("Event name: {:?}", event_name);
            let not_indexed_params = abi_parser.find_event_not_indexed_params(event_name)?;
            println!("Not indexed params: {:?}", not_indexed_params);
            not_indexed_params
        } else {
            None
        };

        // Select the target block based on the input and the current DB status.
        let start_block = if let Some(block) = &args.start_block {
            if let Ok(block_num) = block.parse::<u64>() {
                BlockNumberOrTag::Number(block_num)
            } else {
                BlockNumberOrTag::Latest
            }
        } else {
            BlockNumberOrTag::Latest
        };

        let cancellation_token = CancellationToken::new();

        let storage = if let Some(db_path) = &args.database {
            DuckDBStorage::with_db(&db_path, not_indexed_params.is_some())?
        } else {
            DuckDBStorage::new(not_indexed_params.is_some())?
        };

        let target_block = IndexingApp::choose_target_block(&storage, start_block)?;
        let contract_address = args.contract.parse::<Address>()?;

        let storage_for_api = Arc::new(storage.clone());

        let api_server_address = args
            .api_server
            .unwrap_or(constants::DEFAULT_API_SERVER_ADDRESS.to_string());

        let host_list = vec![args.rpc_host.parse::<RpcHost>()?];
        Ok(Self {
            storage: Arc::new(storage),
            host_list,
            api_server_address,
            storage_for_api,
            cancellation_token,
            events: vec![args.event],
            not_indexed_params,
            start_block: target_block,
            contract_address,
        })
    }

    /// Runs the indexing app.
    pub async fn run(&self) -> Result<()> {
        // Define the tables required for the requested events if needed.
        self.storage
            .include_events(&self.events, self.not_indexed_params.clone())?;

        // Buffer for the event collector and processor.
        let (producer_buffer, consumer_buffer) = mpsc::channel(constants::DEFAULT_INDEXING_BUFFER);

        let event_collector_runner = EventCollectorRunner::new(
            &self.host_list,
            self.contract_address,
            self.events.clone(),
            self.start_block,
            producer_buffer,
        )?;

        let mut event_processor = EventProcessor::new(
            self.storage.clone(),
            self.start_block.as_number().unwrap(),
            consumer_buffer,
            self.cancellation_token.clone(),
        );

        // Start the REST API server
        start_api_server(
            self.api_server_address.as_str(),
            self.storage_for_api.clone(),
            self.cancellation_token.clone(),
        )
        .with_context(|| "Failure in the REST API server")?;

        let _ = tokio::spawn(async move { event_collector_runner.run().await });

        let processor_handle = tokio::spawn(async move { event_processor.run().await });

        let cancellation_token = self.cancellation_token.clone();

        // Spawn a task that handles Ctrl+C and aborts the collector
        let ctrl_c_task = tokio::spawn(async move {
            ctrl_c().await.ok();
            println!("\nReceived Ctrl+C, shutting down gracefully...");
            // Signal cancellation to processor
            cancellation_token.graceful_shutdown();
        });

        // Collector tasks can be safely killed without the token, so these will be dropped automatically.
        let _ = join!(ctrl_c_task, processor_handle);

        println!("Shutdown complete");

        Ok(())
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
}

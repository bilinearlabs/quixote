// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken, EventCollectorRunner, EventProcessor, RpcHost,
    api_rest::start_api_server,
    cli::IndexingArgs,
    constants,
    storage::{DuckDBStorage, DuckDBStorageFactory, Storage},
};
use alloy::{
    eips::BlockNumberOrTag,
    json_abi::{Event, JsonAbi},
    primitives::Address,
};
use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;
use tokio::{join, signal::ctrl_c, sync::mpsc};
use tracing::{info, warn};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub host_list: Vec<RpcHost>,
    pub api_server_address: String,
    pub storage_for_api: Arc<DuckDBStorageFactory>,
    pub cancellation_token: CancellationToken,
    pub events: Vec<Event>,
    pub start_block: BlockNumberOrTag,
    pub contract_address: Address,
}

impl IndexingApp {
    /// Builds a new instance fo the indexing app using the command line arguments.
    pub fn build_app() -> Result<Self> {
        // Parse the command line arguments.
        let args = IndexingArgs::parse();

        // Build a list of events from the command line arguments.
        let events = Self::get_events(&args)?;

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
            DuckDBStorage::with_db(&db_path)?
        } else {
            DuckDBStorage::new()?
        };

        // Register the indexed events in the database if not already registered.
        storage.include_events(events.as_slice())?;

        let target_block = IndexingApp::choose_target_block(&storage, start_block)?;

        let contract_address = args.contract.parse::<Address>()?;

        // Create a factory that can create new storage instances per request
        let db_path = if let Some(db_path) = &args.database {
            db_path.clone()
        } else {
            constants::DUCKDB_FILE_PATH.to_string()
        };
        let storage_for_api = Arc::new(DuckDBStorageFactory::new(db_path));

        let api_server_address = args
            .api_server
            .unwrap_or(constants::DEFAULT_API_SERVER_ADDRESS.to_string());

        info!("Indexing the contract {contract_address} from the block {target_block:?}");

        let host_list = vec![args.rpc_host.parse::<RpcHost>()?];
        Ok(Self {
            storage: Arc::new(storage),
            host_list,
            api_server_address,
            storage_for_api,
            cancellation_token,
            events,
            start_block: target_block,
            contract_address,
        })
    }

    /// Runs the indexing app.
    pub async fn run(&self) -> Result<()> {
        // Define the tables required for the requested events if needed.
        self.storage.include_events(&self.events)?;

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

        info!("REST API server listening on {}", self.api_server_address);

        info!("Starting the indexing of events");

        // Launch teh event processor
        let processor_handle = tokio::spawn(async move { event_processor.run().await });

        // Launch the event collector runner
        let _ = tokio::spawn(async move { event_collector_runner.run().await });

        // Spawn a task that handles Ctrl+C and aborts the collector
        let ctrl_c_task = IndexingApp::spawn_ctrl_c_handler(self.cancellation_token.clone());

        // Collector tasks can be safely killed without the token, so these will be dropped automatically.
        let _ = join!(ctrl_c_task, processor_handle);

        info!("Shutdown complete");

        Ok(())
    }

    fn spawn_ctrl_c_handler(cancellation_token: CancellationToken) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            ctrl_c().await.ok();
            warn!("Received Ctrl+C, shutting down gracefully...");
            // Signal cancellation to processor
            cancellation_token.graceful_shutdown();
        })
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
                    info!("Your database was empty. Setting the first block to {n}");
                    db_conn.set_first_block(n)?;
                    Ok(BlockNumberOrTag::Number(n))
                } else {
                    Ok(BlockNumberOrTag::Number(db_last_block + 1))
                }
            }
            // Continue where the DB left off.
            _ => Ok(BlockNumberOrTag::Number(db_last_block + 1)),
        }
    }

    /// Builds a list of events from the command line arguments.
    fn get_events(args: &IndexingArgs) -> Result<Vec<Event>> {
        if let Some(event) = &args.event {
            if args.abi_spec.is_some() {
                warn!("The given ABI will be ignored. The option -e takes precedence over -a.");
            }

            let event = Event::parse(event).map_err(|_| {
                anyhow::anyhow!("Failed to parse the given event. Use --help for more information.")
            })?;

            Ok(vec![event])
        } else if let Some(abi_spec) = &args.abi_spec {
            let json = std::fs::read_to_string(abi_spec)?;
            let abi: JsonAbi = serde_json::from_str(&json)?;
            let events = abi
                .events()
                .into_iter()
                .map(|e| e.clone())
                .collect::<Vec<Event>>();
            Ok(events)
        } else {
            Err(anyhow::anyhow!(
                "Missing event or ABI spec to index. Use --help for more information."
            ))
        }
    }
}

// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken, CollectorSeed, EventCollectorRunner, EventProcessor, TxLogChunk,
    api_rest::start_api_server,
    configuration::IndexerConfiguration,
    constants, error_codes,
    storage::{DuckDBStorage, DuckDBStorageFactory, Storage},
};
use anyhow::{Context, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::{signal::ctrl_c, sync::mpsc};
use tracing::{error, info, warn};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub api_server_address: String,
    pub storage_for_api: Arc<DuckDBStorageFactory>,
    pub cancellation_token: CancellationToken,
    pub seeds: Vec<CollectorSeed>,
}

impl IndexingApp {
    /// Builds a new instance of the indexing app using the configuration.
    pub async fn build_app(config: &IndexerConfiguration) -> Result<Self> {
        let cancellation_token = CancellationToken::default();

        // Instantiate the DB handlers, for the consumer task and the API server.
        let (storage, storage_for_api) = {
            let mut storage = DuckDBStorage::with_db(&config.database_path)?;
            storage.set_strict_mode(config.strict_mode);
            (
                storage,
                Arc::new(DuckDBStorageFactory::new(config.database_path.clone())),
            )
        };

        // Build a list of events from the command line arguments.
        let seeds = match CollectorSeed::build_seeds(&storage, config).await {
            Ok(seeds) => seeds,
            Err(e) => {
                error!("Failed to build the indexing jobs: {}", e);
                std::process::exit(error_codes::ERROR_CODE_WRONG_INPUT_ARGUMENTS);
            }
        };

        let api_server_address =
            format!("{}:{}", config.api_server_address, config.api_server_port);

        Ok(Self {
            storage: Arc::new(storage),
            api_server_address,
            storage_for_api,
            cancellation_token,
            seeds,
        })
    }

    /// Runs the indexing app.
    ///
    /// # Description
    ///
    /// This method creates one EventProcessor per unique chain_id. Each chain has its own
    /// buffer channel, ensuring that block ordering is maintained within each chain.
    pub async fn run(&self) -> Result<()> {
        // Group seeds by chain_id to determine unique chains and their start blocks
        let mut chain_info: HashMap<u64, u64> = HashMap::new(); // chain_id -> start_block
        for seed in &self.seeds {
            chain_info
                .entry(seed.chain_id)
                .and_modify(|existing_start| {
                    // Validate that all seeds for the same chain have the same start_block
                    if *existing_start != seed.start_block {
                        error!(
                            "Chain {:#x}: Seeds have different start blocks ({} vs {}). Resuming from such DB is not supported yet.",
                            seed.chain_id, *existing_start, seed.start_block
                        );
                        std::process::exit(error_codes::ERROR_CODE_BAD_DB_STATE);
                    }
                })
                .or_insert(seed.start_block);
        }

        info!("Found {} unique chain(s) to index", chain_info.len());

        // Create per-chain buffers: chain_id -> (tx, rx)
        let mut chain_buffers: HashMap<u64, TxLogChunk> = HashMap::new();
        let mut processor_handles = Vec::new();

        for (chain_id, start_block) in &chain_info {
            let (producer_buffer, consumer_buffer) =
                mpsc::channel(constants::DEFAULT_INDEXING_BUFFER);
            chain_buffers.insert(*chain_id, producer_buffer);

            // Create an EventProcessor for this chain
            let mut event_processor = EventProcessor::new(
                *chain_id,
                self.storage.clone(),
                *start_block,
                consumer_buffer,
                self.cancellation_token.clone(),
            );

            info!(
                "Starting EventProcessor for chain {:#x} from block {}",
                chain_id, start_block
            );

            let handle = tokio::spawn(async move { event_processor.run().await });
            processor_handles.push(handle);
        }

        // Create the event collector runner with per-chain buffers
        let event_collector_runner = EventCollectorRunner::new(self.seeds.clone(), chain_buffers)?;

        // Start the REST API server
        start_api_server(
            self.api_server_address.as_str(),
            self.storage_for_api.clone(),
            self.cancellation_token.clone(),
        )
        .await
        .with_context(|| "Failure in the REST API server")?;

        info!("Starting the indexing of events");

        // Launch the event collector runner
        #[allow(clippy::let_underscore_future)]
        let _ = tokio::spawn(async move { event_collector_runner.run().await });

        // Spawn a task that handles Ctrl+C and aborts the collector
        let ctrl_c_task = IndexingApp::spawn_ctrl_c_handler(self.cancellation_token.clone());

        // Wait for Ctrl+C handler
        ctrl_c_task.await?;

        // Wait for all processors to finish
        for handle in processor_handles {
            let _ = handle.await;
        }

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
}

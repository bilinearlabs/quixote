// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken, CollectorSeed, EventCollectorRunner, EventProcessor,
    api_rest::start_api_server,
    configuration::IndexerConfiguration,
    constants, error_codes,
    storage::{DuckDBStorage, DuckDBStorageFactory, Storage},
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::{join, signal::ctrl_c, sync::mpsc};
use tracing::{error, info, warn};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub api_server_address: String,
    pub storage_for_api: Arc<DuckDBStorageFactory>,
    pub cancellation_token: CancellationToken,
    pub seeds: Vec<CollectorSeed>,
    pub default_block_range: usize,
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
        let default_block_range = config
            .index_jobs
            .first()
            .unwrap()
            .block_range
            .unwrap_or(constants::DEFAULT_BLOCK_RANGE);

        Ok(Self {
            storage: Arc::new(storage),
            api_server_address,
            storage_for_api,
            cancellation_token,
            seeds,
            default_block_range,
        })
    }

    /// Runs the indexing app.
    pub async fn run(&self) -> Result<()> {
        // Buffer for the event collector and processor.
        let (producer_buffer, consumer_buffer) = mpsc::channel(constants::DEFAULT_INDEXING_BUFFER);

        let start_block = self.seeds.first().unwrap().start_block;

        self.seeds.iter().for_each(|seed| {
            if seed.start_block != start_block {
                error!("Your DB contains events that are synchronized up to different blocks. Resuming from such DB is not supported yet.");
                std::process::exit(error_codes::ERROR_CODE_BAD_DB_STATE);
            }
        });

        let event_collector_runner = EventCollectorRunner::new(
            self.seeds.clone(),
            producer_buffer,
            self.default_block_range,
        )?;

        let mut event_processor = EventProcessor::new(
            self.storage.clone(),
            start_block,
            consumer_buffer,
            self.cancellation_token.clone(),
        );

        // Start the REST API server
        start_api_server(
            self.api_server_address.as_str(),
            self.storage_for_api.clone(),
            self.cancellation_token.clone(),
        )
        .await
        .with_context(|| "Failure in the REST API server")?;

        info!("Starting the indexing of events");

        // Launch teh event processor
        let processor_handle = tokio::spawn(async move { event_processor.run().await });

        // Launch the event collector runner
        #[allow(clippy::let_underscore_future)]
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
}

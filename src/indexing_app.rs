// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

use crate::{
    CancellationToken, CollectorSeed, EventCollectorRunner, EventProcessor, TxLogChunk,
    api_rest::start_api_server,
    configuration::IndexerConfiguration,
    constants, error_codes,
    metrics::{MetricsConfig, MetricsHandle},
    storage::{DuckDBStorage, Storage, StorageFactory},
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::{signal::ctrl_c, sync::mpsc};
use tracing::{error, info, warn};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub api_server_address: String,
    pub storage_for_api: Arc<dyn StorageFactory>,
    pub cancellation_token: CancellationToken,
    pub seeds: Vec<CollectorSeed>,
    pub metrics_config: MetricsConfig,
    pub metrics: MetricsHandle,
}

impl IndexingApp {
    /// Builds a new instance of the indexing app using the configuration.
    pub async fn build_app(config: &IndexerConfiguration) -> Result<Self> {
        let cancellation_token = CancellationToken::default();

        let metrics_config = MetricsConfig {
            enabled: config.metrics,
            address: config.metrics_address.clone(),
            port: config.metrics_port,
            allow_origin: config.metrics_allow_origin.clone(),
        };
        let metrics = MetricsHandle::new(&metrics_config)?;

        // Instantiate the DB handlers, for the consumer task and the API server.
        let (storage, storage_for_api): (DuckDBStorage, Arc<dyn StorageFactory>) = {
            let mut storage = DuckDBStorage::with_db(&config.database_path)?;
            storage.set_strict_mode(config.strict_mode);
            let storage_clone = storage.clone();
            (storage, Arc::new(storage_clone))
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
            metrics_config,
            metrics,
        })
    }

    /// Runs the indexing app.
    ///
    /// # Description
    ///
    /// This method creates one EventProcessor per seed. Each seed is an isolated unit of work
    /// with its own collector and processor, ensuring independent processing regardless of chain_id.
    pub async fn run(&self) -> Result<()> {
        if self.metrics.is_enabled() {
            let _ = self
                .metrics
                .serve(self.metrics_config.clone())
                .await
                .with_context(|| "Failed to start metrics server")?;
        }
        info!("Starting {} indexing job(s)", self.seeds.len());

        // Create one buffer and one processor per seed
        let mut seed_buffers: Vec<TxLogChunk> = Vec::new();
        let mut processor_handles = Vec::new();

        for (seed_index, seed) in self.seeds.iter().enumerate() {
            let (producer_buffer, consumer_buffer) =
                mpsc::channel(constants::DEFAULT_INDEXING_BUFFER);
            seed_buffers.push(producer_buffer);

            // Extract event selectors from the seed's events
            let event_selectors: Vec<_> = seed.events.iter().map(|e| e.selector()).collect();

            // Create an EventProcessor for this seed
            let mut event_processor = EventProcessor::new(
                seed.chain_id,
                seed.contract_address,
                self.storage.clone(),
                seed.start_block,
                consumer_buffer,
                self.cancellation_token.clone(),
                self.metrics.clone(),
                event_selectors,
            );

            info!(
                "Starting EventProcessor {} for chain {:#x}, contract {}, from block {}, with {} event(s)",
                seed_index,
                seed.chain_id,
                seed.contract_address,
                seed.start_block,
                seed.events.len()
            );

            let handle = tokio::spawn(async move { event_processor.run().await });
            processor_handles.push(handle);
        }

        // Create the event collector runner with per-seed buffers
        let event_collector_runner =
            EventCollectorRunner::new(self.seeds.clone(), seed_buffers, self.metrics.clone())?;

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

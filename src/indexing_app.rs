// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::{
    CancellationToken, CollectorSeed, EventCollectorRunner, EventProcessor, RpcHost,
    api_rest::start_api_server,
    cli::IndexingArgs,
    constants, error_codes,
    storage::{DuckDBStorage, DuckDBStorageFactory, Storage},
};
use alloy::{
    eips::BlockNumberOrTag,
    json_abi::{Event, JsonAbi},
    primitives::Address,
    rpc::types::Filter,
};
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::{join, signal::ctrl_c, sync::mpsc};
use tracing::{error, info, warn};

pub struct IndexingApp {
    pub storage: Arc<dyn Storage + Send + Sync>,
    pub host_list: Vec<RpcHost>,
    pub api_server_address: String,
    pub storage_for_api: Arc<DuckDBStorageFactory>,
    pub cancellation_token: CancellationToken,
    pub seeds: Vec<CollectorSeed>,
    pub default_block_range: usize,
}

impl IndexingApp {
    /// Builds a new instance fo the indexing app using the command line arguments.
    pub fn build_app(args: IndexingArgs) -> Result<Self> {
        let cancellation_token = CancellationToken::default();

        // Instantiate the DB handlers, for the consumer task and the API server.
        let (storage, storage_for_api) = if let Some(db_path) = &args.database {
            (
                DuckDBStorage::with_db(db_path)?,
                Arc::new(DuckDBStorageFactory::new(db_path.clone())),
            )
        } else {
            (
                DuckDBStorage::new()?,
                Arc::new(DuckDBStorageFactory::new(
                    constants::DUCKDB_FILE_PATH.to_string(),
                )),
            )
        };

        // Build a list of events from the command line arguments.
        let seeds = Self::build_seeds(&storage, &args)?;

        // Define the tables for the requested events.
        for seed in seeds.iter() {
            storage.include_events(seed.contract_address, seed.events.as_slice())?;
        }

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
            seeds,
            default_block_range: args.block_range,
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
            &self.host_list,
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

    /// This static method builds a list of CollectorSeed ready to be taken by the EventCollectorRunner.
    ///
    /// # Description
    ///
    /// When running in ABI mode, the method will build several seeds, based on the amount of events defined in the ABI.
    /// If the ABI includes more than `constants::DEFAULT_USE_FILTERS_THRESHOLD` events, multiple seeds will be created
    /// to spread the load between several RPCs when possible. Otherwise, a single seed will be created for all the
    /// events defined in the ABI.
    ///
    /// When the option `--event` is used, the method will build a single seed for each event specified in the
    /// command line arguments.
    fn build_seeds(conn: &DuckDBStorage, args: &IndexingArgs) -> Result<Vec<CollectorSeed>> {
        // Firsts, check that the given address for the contract is valid.
        let contract_address = args.contract.parse::<Address>().unwrap_or_else(|_| {
            error!(
                "Failed to parse the given contract address: {}",
                args.contract
            );
            std::process::exit(error_codes::ERROR_CODE_WRONG_INPUT_ARGUMENTS);
        });

        let mut seeds = Vec::new();

        // Single events: coming from the --event option.
        if let Some(events) = &args.event {
            for event in events {
                let parsed_event = Event::parse(event).map_err(|_| {
                    anyhow::anyhow!(
                        "Failed to parse the string {event} as a valid event. Use the canonical format of the event as declared in the ABI.",
                    )
                })?;

                let seed = Self::build_single_event_seed(
                    conn,
                    &parsed_event,
                    contract_address,
                    args.start_block.as_deref(),
                )?;
                seeds.push(seed);
            }
        // Multiple events: coming from the --abi option.
        } else if let Some(abi_spec) = &args.abi_spec {
            let json = std::fs::read_to_string(abi_spec)?;
            let abi: JsonAbi = serde_json::from_str(&json)?;
            let events = abi.events().cloned().collect::<Vec<Event>>();

            // Attempt to register the events of the ABI, when existing the operation will be ignored.
            conn.include_events(contract_address, events.as_slice())?;

            let start_block = Self::set_start_block_for_events(
                conn,
                events.as_slice(),
                args.start_block.as_deref(),
            )?;

            // Group all the events into a single seed with no filters.
            if events.len() > constants::DEFAULT_USE_FILTERS_THRESHOLD {
                let len = events.len();
                seeds.push(CollectorSeed {
                    contract_address,
                    events,
                    start_block,
                    sync_mode: BlockNumberOrTag::Finalized,
                    filter: None,
                });
                info!("Indexing {len} events of the ABI from the block {start_block}");
            // When the number of events is less than the threshold, use filters for each event.
            // As if the user would have used the --event option for each event.
            } else {
                for event in events {
                    let seed = Self::build_single_event_seed(
                        conn,
                        &event,
                        contract_address,
                        args.start_block.as_deref(),
                    )?;
                    seeds.push(seed);
                }
            }
        } else {
            error!("Missing event or ABI spec to index. Use --help for more information.");
            std::process::exit(error_codes::ERROR_CODE_WRONG_INPUT_ARGUMENTS);
        }

        Ok(seeds)
    }

    /// Gets the start block for an event and ensures the first block is set if needed.
    ///
    /// This method checks if the event was already indexed. If the DB contains the event,
    /// it resumes from the last synchronized block. If not, it uses the start block from
    /// the command line arguments. It also ensures the first_block is set if the DB was empty.
    fn set_start_block_for_events(
        conn: &DuckDBStorage,
        events: &[Event],
        start_block_arg: Option<&str>,
    ) -> Result<u64> {
        // Let's check if this event was already indexed.
        // IF the DB contains the event, resume from the last synchronized block.
        // If not, consider the start block from the command line arguments.
        let event = events.first().unwrap();

        let start_block = if let Ok(last_block) = conn.last_block(event) {
            if last_block != 0 {
                info!(
                    "The DB contains events of the ABI up to the block {last_block}. Resuming indexing from the last synchronized block."
                );
                last_block.saturating_add(1)
            } else {
                info!("The DB is empty for the events included in the ABI.");
                let start_block = start_block_arg
                    .unwrap_or_default()
                    .parse::<u64>()
                    .context("Failed to parse start_block argument")?;

                for event in events {
                    conn.set_first_block(event, start_block)?;
                }

                start_block
            }
        } else {
            error!(
                "Failed to access the sync state for the event {}. Check the integrity of the database.",
                event.name
            );
            std::process::exit(error_codes::ERROR_CODE_BAD_DB_STATE);
        };

        Ok(start_block)
    }

    /// Builds a CollectorSeed for a single event with a filter.
    ///
    /// This method registers the event, gets/sets the sync state, and creates a seed
    /// with a filter for efficient event retrieval.
    fn build_single_event_seed(
        conn: &DuckDBStorage,
        event: &Event,
        contract_address: Address,
        start_block_arg: Option<&str>,
    ) -> Result<CollectorSeed> {
        // Is the event already indexed?
        let event_status = conn.event_index_status(event)?;

        let start_block = if let Some(event_status) = event_status {
            info!(
                "The event {} contains {} entries in the DB, from the block {} to the block {}. Resuming indexing from the last synchronized block.",
                event.name,
                event_status.event_count,
                event_status.first_block,
                event_status.last_block
            );
            event_status.last_block.saturating_add(1)
        } else {
            let first_block = start_block_arg
                .unwrap_or_default()
                .parse::<u64>()
                .unwrap_or_default();
            info!(
                "The event {} will be registered in the DB and indexed from the block {first_block}",
                event.name
            );
            conn.include_events(contract_address, std::slice::from_ref(event))?;
            conn.set_first_block(event, first_block)?;
            first_block
        };

        // Let's build the filter that will be used by eth_getLogs to retrieve the data.
        let filter = Some(
            Filter::new()
                .address(contract_address)
                .event_signature(event.selector()),
        );

        Ok(CollectorSeed {
            contract_address,
            events: vec![event.clone()],
            start_block,
            sync_mode: BlockNumberOrTag::Finalized,
            filter,
        })
    }
}

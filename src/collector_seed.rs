// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the collector seed.
//!
//! # Description
//!
//! Seeds describe the minimal execution unit for an indexing task. Each seed is associated with a contract address
//! and a set of events. All the events within the same seed must maintain a coherent indexing state in the database.
//! This means the indexer won't be able to resume an indexing task from a previous run if the events are disjoint,
//! i.e. they synchronized up to different blocks if the ABI mode is selected.

use crate::{
    configuration::IndexerConfiguration,
    storage::{DuckDBStorage, Storage},
};
use alloy::{
    eips::BlockNumberOrTag,
    json_abi::{Event, JsonAbi},
    primitives::Address,
    providers::{Provider, ProviderBuilder},
    rpc::{client::RpcClient, types::Filter},
    transports::http::reqwest::Url,
};
use anyhow::Result;
use tracing::{debug, info};

/// Object that represents a seed for a collecting job.
///
/// # Description
///
/// This object includes all the information needed to start an indexing job. The indexing job may fetch one or many
/// events, but as they all belong to the same job, they all need to synchronize up to the same block.
///
/// All input data is validated during construction to ensure the seed is ready to use.
#[derive(Debug, Clone)]
pub struct CollectorSeed {
    /// The chain ID of the network where the contract is deployed.
    pub chain_id: u64,
    /// The RPC URL to connect to the network (already parsed and validated).
    pub rpc_url: Url,
    /// The contract address to index.
    pub contract_address: Address,
    /// The events to index from the contract.
    pub events: Vec<Event>,
    /// The block number to start indexing from.
    pub start_block: u64,
    /// The sync mode for the indexing job.
    pub sync_mode: BlockNumberOrTag,
    /// Optional filter for the events.
    pub filter: Option<Filter>,
}

impl CollectorSeed {
    /// Create a new CollectorSeed object.
    ///
    /// # Description
    ///
    /// This function creates a new CollectorSeed object. It checks if the given events are synchronized up to the same
    /// block and returns a new CollectorSeed object.
    ///
    /// TODO: fully support filters
    pub async fn new(
        db_conn: &DuckDBStorage,
        chain_id: u64,
        rpc_url: Url,
        contract_address: Address,
        events: Vec<Event>,
        start_block: u64,
        filter: Option<Filter>,
    ) -> Result<Self> {
        // Ensure the given set of events are synchronized up to the same block.
        let stored_start_block = Self::check_start_block(db_conn, &events).await?;

        // If the DB is empty, consider the given start_block, if not resume from the last
        // stored block.
        let start_block = if stored_start_block == 0 {
            start_block
        } else {
            stored_start_block
        };

        Ok(Self {
            chain_id,
            rpc_url,
            contract_address,
            events,
            start_block,
            sync_mode: BlockNumberOrTag::Finalized,
            filter,
        })
    }

    /// Ensure all the given events are synchronized up to the same block.
    async fn check_start_block(db_conn: &DuckDBStorage, events: &[Event]) -> Result<u64> {
        // Check that all the events synchronized the same blocks.
        // If a previous run indexed events using -e multiple times, and the current run is
        // using the -a option, the indexing state for such event might be different. Thus
        // we can't resume indexing these events as a block (using the same eth_getLogs call).
        let current_start_block = db_conn.first_block(events.iter().next().unwrap())?;
        for event in events {
            if current_start_block != db_conn.first_block(event)? {
                return Err(anyhow::anyhow!(
                    "The given events are disjoint. This means that you need to run the indexer using -e for each one of the given events."
                ));
            }
        }

        Ok(current_start_block)
    }

    /// Gets the start block for an event and ensures the first block is set if needed.
    ///
    /// This method checks if the event was already indexed. If the DB contains the event,
    /// it resumes from the last synchronized block. If not, it uses the start block from
    /// the command line arguments. It also ensures the first_block is set if the DB was empty.
    fn set_start_block_for_events(
        conn: &DuckDBStorage,
        events: &[Event],
        start_block_arg: Option<u64>,
    ) -> Result<u64> {
        // Let's check if this event was already indexed.
        // IF the DB contains the event, resume from the last synchronized block.
        // If not, consider the start block from the command line arguments.
        let event = events.first().unwrap();

        if let Ok(last_block) = conn.last_block(event) {
            if last_block != 0 {
                info!(
                    "The DB contains events of the ABI up to the block {last_block}. Resuming indexing from the last synchronized block."
                );
                Ok(last_block.saturating_add(1))
            } else {
                info!("The DB is empty for the events included in the ABI.");
                let start_block = start_block_arg.unwrap_or_default();

                for event in events {
                    conn.set_first_block(event, start_block)?;
                }

                Ok(start_block)
            }
        } else {
            Err(anyhow::anyhow!(
                "Failed to access the sync state for the event {}. Check the integrity of the database.",
                event.name
            ))
        }
    }

    /// Fetches the chain_id from the given RPC URL.
    ///
    /// # Description
    ///
    /// Creates a temporary provider to query the chain_id from the RPC endpoint.
    pub async fn fetch_chain_id(rpc_url: String) -> Result<u64, anyhow::Error> {
        let url: Url = rpc_url
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse RPC URL '{}': {}", rpc_url, e))?;

        let provider = ProviderBuilder::new().connect_client(RpcClient::builder().http(url));
        provider
            .get_chain_id()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch chain_id from RPC '{}': {}", rpc_url, e))
    }

    /// Builds a list of CollectorSeed from the configuration's index jobs.
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
    pub async fn build_seeds(
        db_conn: &DuckDBStorage,
        config: &IndexerConfiguration,
    ) -> Result<Vec<Self>, anyhow::Error> {
        Self::build_seeds_with_chain_id_resolver(db_conn, config, Self::fetch_chain_id).await
    }

    /// Builds a list of CollectorSeed from the configuration's index jobs with a custom chain_id resolver.
    ///
    /// # Description
    ///
    /// This method allows injecting a custom chain_id resolver, which is useful for testing
    /// without making real RPC calls.
    pub async fn build_seeds_with_chain_id_resolver<F, Fut>(
        db_conn: &DuckDBStorage,
        config: &IndexerConfiguration,
        chain_id_resolver: F,
    ) -> Result<Vec<Self>, anyhow::Error>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Result<u64, anyhow::Error>>,
    {
        let mut job_seeds = Vec::new();

        for job in config.index_jobs.iter() {
            // Parse and validate the RPC URL
            let rpc_url: Url = job
                .rpc_url
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse RPC URL '{}': {}", job.rpc_url, e))?;

            // Fetch chain_id from the RPC
            let chain_id = chain_id_resolver(job.rpc_url.clone()).await?;

            // Parse the contract address
            let contract_address = job.contract.parse::<Address>().map_err(|_| {
                anyhow::anyhow!(format!(
                    "Failed to parse the given contract address: {}",
                    job.contract
                ))
            })?;

            let mut coming_from_abi = false;

            // Parse the given event signatures into [alloy::json_abi::Event]
            let events = if let Some(abi_path) = job.abi_path.as_ref() {
                let json = std::fs::read_to_string(abi_path).map_err(|e| {
                    anyhow::anyhow!("Failed to read the ABI file '{}': {}", abi_path, e)
                })?;
                let abi: JsonAbi = serde_json::from_str(&json).map_err(|e| {
                    anyhow::anyhow!("Failed to parse the ABI file '{}': {}", abi_path, e)
                })?;
                coming_from_abi = true;
                abi.events().cloned().collect::<Vec<Event>>()
            } else if let Some(events) = job.events.as_ref() {
                events
                    .iter()
                    .map(|event| {
                        Event::parse(event.full_signature.as_str()).map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to parse the event signature '{}': {}",
                                event.full_signature,
                                e
                            )
                        })
                    })
                    .collect::<Result<Vec<Event>, _>>()?
            } else {
                return Err(anyhow::anyhow!(
                    "Missing event definition for the indexing job. Indexing a contract with no event definition is not supported."
                ));
            };

            // Register the events in the DB. If the events are already registered, the operation will be ignored.
            db_conn.include_events(&events)?;
            // Now, ensure all these events are synchronized up to the same block.
            let start_block = Self::check_start_block(db_conn, &events).await?;

            // If the DB is empty, initialize the DB to start indexing from the given start block.
            // Otherwise, resume from the last synchronized block.
            let start_block = if start_block == 0 {
                Self::set_start_block_for_events(db_conn, &events, job.start_block)?
            } else {
                start_block
            };

            let filter = if coming_from_abi {
                None
            } else {
                Some(
                    Filter::new()
                        .address(contract_address)
                        .event_signature(events.first().unwrap().selector()),
                )
            };

            let seed = Self {
                chain_id,
                rpc_url,
                contract_address,
                events,
                start_block,
                sync_mode: BlockNumberOrTag::Finalized,
                filter,
            };

            info!(
                "Created indexing job for chain {} at {} for contract {} with {} event(s), starting from block {}",
                seed.chain_id,
                seed.rpc_url,
                seed.contract_address,
                seed.events.len(),
                seed.start_block
            );
            debug!(
                "Seed details: events=[{}], sync_mode={:?}, filter={}",
                seed.events
                    .iter()
                    .map(|e| e.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                seed.sync_mode,
                if seed.filter.is_some() {
                    "enabled"
                } else {
                    "disabled"
                }
            );

            job_seeds.push(seed);
        }

        Ok(job_seeds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configuration::{EventJob, IndexJob};
    use alloy::json_abi::Event;
    use pretty_assertions::assert_eq;
    use rstest::{fixture, rstest};

    const TEST_CONTRACT_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
    const TEST_RPC_URL: &str = "http://localhost:8545/";
    const TEST_CHAIN_ID: u64 = 1;
    const TRANSFER_EVENT_SIGNATURE: &str =
        "Transfer(address indexed from, address indexed to, uint256 value)";
    const APPROVAL_EVENT_SIGNATURE: &str =
        "Approval(address indexed owner, address indexed spender, uint256 value)";

    /// Mock chain_id resolver that returns a fixed chain_id without making RPC calls.
    async fn mock_chain_id_resolver(_rpc_url: String) -> Result<u64, anyhow::Error> {
        Ok(TEST_CHAIN_ID)
    }

    /// Helper to get the expected parsed URL for assertions.
    fn expected_rpc_url() -> Url {
        TEST_RPC_URL.parse().unwrap()
    }

    /// Creates an in-memory DuckDB storage for testing.
    #[fixture]
    fn test_db() -> DuckDBStorage {
        DuckDBStorage::with_db(":memory:").expect("in-memory DB should open")
    }

    /// Creates an IndexerConfiguration with a single event (Transfer).
    #[fixture]
    fn config_single_event() -> IndexerConfiguration {
        IndexerConfiguration {
            index_jobs: vec![IndexJob {
                rpc_url: "http://localhost:8545".to_string(),
                contract: TEST_CONTRACT_ADDRESS.to_string(),
                start_block: Some(0),
                block_range: Some(1000),
                events: Some(vec![EventJob {
                    full_signature: TRANSFER_EVENT_SIGNATURE.to_string(),
                    filters: None,
                }]),
                abi_path: None,
            }],
            database_path: ":memory:".to_string(),
            api_server_address: "127.0.0.1".to_string(),
            api_server_port: 9720,
            frontend_address: "127.0.0.1".to_string(),
            frontend_port: 8501,
            verbosity: 0,
            disable_frontend: true,
            strict_mode: false,
        }
    }

    /// Creates an IndexerConfiguration with an ABI file (USDC).
    #[fixture]
    fn config_with_abi() -> IndexerConfiguration {
        IndexerConfiguration {
            index_jobs: vec![IndexJob {
                rpc_url: "http://localhost:8545".to_string(),
                contract: TEST_CONTRACT_ADDRESS.to_string(),
                start_block: Some(0),
                block_range: Some(1000),
                events: None,
                abi_path: Some("test/fixtures/usdc_abi.json".to_string()),
            }],
            database_path: ":memory:".to_string(),
            api_server_address: "127.0.0.1".to_string(),
            api_server_port: 9720,
            frontend_address: "127.0.0.1".to_string(),
            frontend_port: 8501,
            verbosity: 0,
            disable_frontend: true,
            strict_mode: false,
        }
    }

    /// Creates an IndexerConfiguration with two separate events (simulating -e flag used twice).
    #[fixture]
    fn config_two_events() -> IndexerConfiguration {
        IndexerConfiguration {
            index_jobs: vec![
                IndexJob {
                    rpc_url: "http://localhost:8545".to_string(),
                    contract: TEST_CONTRACT_ADDRESS.to_string(),
                    start_block: Some(0),
                    block_range: Some(1000),
                    events: Some(vec![EventJob {
                        full_signature: TRANSFER_EVENT_SIGNATURE.to_string(),
                        filters: None,
                    }]),
                    abi_path: None,
                },
                IndexJob {
                    rpc_url: "http://localhost:8545".to_string(),
                    contract: TEST_CONTRACT_ADDRESS.to_string(),
                    start_block: Some(0),
                    block_range: Some(1000),
                    events: Some(vec![EventJob {
                        full_signature: APPROVAL_EVENT_SIGNATURE.to_string(),
                        filters: None,
                    }]),
                    abi_path: None,
                },
            ],
            database_path: ":memory:".to_string(),
            api_server_address: "127.0.0.1".to_string(),
            api_server_port: 9720,
            frontend_address: "127.0.0.1".to_string(),
            frontend_port: 8501,
            verbosity: 0,
            disable_frontend: true,
            strict_mode: false,
        }
    }

    /// Test case 1: For an input config that defines a single event (Transfer),
    /// build_seeds must create a single seed with a filter for its signature and the given contract.
    #[rstest]
    #[tokio::test]
    async fn build_seeds_single_event_creates_one_seed_with_filter(
        test_db: DuckDBStorage,
        config_single_event: IndexerConfiguration,
    ) {
        let seeds = CollectorSeed::build_seeds_with_chain_id_resolver(
            &test_db,
            &config_single_event,
            mock_chain_id_resolver,
        )
        .await
        .expect("build_seeds should succeed");

        // Should create exactly one seed
        assert_eq!(
            seeds.len(),
            1,
            "Expected exactly one seed for single event config"
        );

        let seed = &seeds[0];

        // Verify the chain_id and rpc_url
        assert_eq!(seed.chain_id, TEST_CHAIN_ID);
        assert_eq!(seed.rpc_url, expected_rpc_url());

        // Verify the contract address
        let expected_address: Address = TEST_CONTRACT_ADDRESS.parse().unwrap();
        assert_eq!(seed.contract_address, expected_address);

        // Verify that exactly one event is included
        assert_eq!(
            seed.events.len(),
            1,
            "Expected exactly one event in the seed"
        );

        // Verify the event is the Transfer event
        let transfer_event = Event::parse(TRANSFER_EVENT_SIGNATURE).unwrap();
        assert_eq!(seed.events[0].name, transfer_event.name);
        assert_eq!(seed.events[0].selector(), transfer_event.selector());

        // Verify that a filter is set (since it's a single event, not from ABI)
        assert!(
            seed.filter.is_some(),
            "Expected filter to be set for single event config"
        );
    }

    /// Test case 2: For an input config with an ABI file (USDC),
    /// a single seed must be created with no filters.
    #[rstest]
    #[tokio::test]
    async fn build_seeds_with_abi_creates_one_seed_without_filter(
        test_db: DuckDBStorage,
        config_with_abi: IndexerConfiguration,
    ) {
        let seeds = CollectorSeed::build_seeds_with_chain_id_resolver(
            &test_db,
            &config_with_abi,
            mock_chain_id_resolver,
        )
        .await
        .expect("build_seeds should succeed");

        // Should create exactly one seed
        assert_eq!(seeds.len(), 1, "Expected exactly one seed for ABI config");

        let seed = &seeds[0];

        // Verify the chain_id and rpc_url
        assert_eq!(seed.chain_id, TEST_CHAIN_ID);
        assert_eq!(seed.rpc_url, expected_rpc_url());

        // Verify the contract address
        let expected_address: Address = TEST_CONTRACT_ADDRESS.parse().unwrap();
        assert_eq!(seed.contract_address, expected_address);

        // Verify that multiple events are included (USDC ABI has many events)
        assert!(
            seed.events.len() > 1,
            "Expected multiple events from ABI, got {}",
            seed.events.len()
        );

        // Verify that no filter is set (ABI mode should not use filters)
        assert!(
            seed.filter.is_none(),
            "Expected no filter for ABI-based config"
        );
    }

    /// Test case 3: For an input config that defines 2 separate events (simulating -e used multiple times),
    /// 2 seeds must be created, each with its own filter.
    #[rstest]
    #[tokio::test]
    async fn build_seeds_two_events_creates_two_seeds_with_filters(
        test_db: DuckDBStorage,
        config_two_events: IndexerConfiguration,
    ) {
        let seeds = CollectorSeed::build_seeds_with_chain_id_resolver(
            &test_db,
            &config_two_events,
            mock_chain_id_resolver,
        )
        .await
        .expect("build_seeds should succeed");

        // Should create exactly two seeds
        assert_eq!(
            seeds.len(),
            2,
            "Expected exactly two seeds for two-event config"
        );

        let expected_address: Address = TEST_CONTRACT_ADDRESS.parse().unwrap();
        let transfer_event = Event::parse(TRANSFER_EVENT_SIGNATURE).unwrap();
        let approval_event = Event::parse(APPROVAL_EVENT_SIGNATURE).unwrap();

        // Verify first seed (Transfer event)
        let first_seed = &seeds[0];
        assert_eq!(first_seed.chain_id, TEST_CHAIN_ID);
        assert_eq!(first_seed.rpc_url, expected_rpc_url());
        assert_eq!(first_seed.contract_address, expected_address);
        assert_eq!(
            first_seed.events.len(),
            1,
            "First seed should have exactly one event"
        );
        assert_eq!(first_seed.events[0].name, transfer_event.name);
        assert_eq!(first_seed.events[0].selector(), transfer_event.selector());
        assert!(
            first_seed.filter.is_some(),
            "First seed should have a filter"
        );

        // Verify second seed (Approval event)
        let second_seed = &seeds[1];
        assert_eq!(second_seed.chain_id, TEST_CHAIN_ID);
        assert_eq!(second_seed.rpc_url, expected_rpc_url());
        assert_eq!(second_seed.contract_address, expected_address);
        assert_eq!(
            second_seed.events.len(),
            1,
            "Second seed should have exactly one event"
        );
        assert_eq!(second_seed.events[0].name, approval_event.name);
        assert_eq!(second_seed.events[0].selector(), approval_event.selector());
        assert!(
            second_seed.filter.is_some(),
            "Second seed should have a filter"
        );
    }
}

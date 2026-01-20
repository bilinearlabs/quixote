// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Library of the Quixote crate.

pub mod event_collector_runner;
pub use event_collector_runner::EventCollectorRunner;
pub mod event_collector;
pub mod event_processor;
pub use event_processor::EventProcessor;
pub mod collector_seed;
pub use collector_seed::CollectorSeed;
pub mod api_rest;
pub mod cli;
pub mod indexing_app;
pub use indexing_app::IndexingApp;
pub mod configuration;
pub mod metrics;
pub mod streamlit_wrapper;
pub use configuration::IndexerConfiguration;
pub mod telemetry;

/// Module with constants used throughout the application.
pub mod constants {
    /// Enables back pressure for the indexing buffer, as producers might overwhelm the buffer when the RPC server is powerful.
    pub const DEFAULT_INDEXING_BUFFER: usize = 10;
    /// Base address for the API server that runs locally.
    pub const DEFAULT_API_SERVER_ADDRESS: &str = "127.0.0.1:9720";
    /// Default poll interval in seconds when indexing the latest block.
    pub const DEFAULT_POLL_INTERVAL: u64 = 1;

    /// Default block range for the event collector. This range is used in the get_Logs call.
    pub const DEFAULT_BLOCK_RANGE: usize = 100;
    /// Maximum number of collector tasks to spawn.
    pub const MAX_CONCURRENT_COLLECTOR_TASKS: usize = 1;
    /// Maximum concurrent requests sent to the RPC server.
    pub const MAX_CONCURRENT_RPC_REQUESTS: usize = 4;
    /// Maximum number of retries for the event collector.
    pub const DEFAULT_BACKOFF_LAYER_MAX_RETRIES: u32 = 100;
    /// Backoff time in milliseconds for the event collector.
    pub const DEFAULT_BACKOFF_LAYER_BACKOFF_TIME: u64 = 2000;
    /// Cup size for the event collector.
    pub const DEFAULT_BACKOFF_LAYER_CUP_SIZE: u64 = 100;
    /// Path to the DuckDB database file.
    pub const DUCKDB_FILE_PATH: &str = "quixote_indexer.duckdb";
    /// Schema version for the DuckDB database.
    pub const DUCKDB_SCHEMA_VERSION: &str = "0.1.0";
    /// Base table name for the DuckDB database.
    pub const DUCKDB_BASE_TABLE_NAME: &str = "quixote_info";
    /// Batch size for bulk INSERT operations in DuckDB.
    pub const DUCKDB_BATCH_INSERT_SIZE: usize = 500;
    /// Value to choose between using filters or not on eth_getLogs.
    pub const DEFAULT_USE_FILTERS_THRESHOLD: usize = 3;
    /// Multiplier for the chunk size.
    pub const CHUNK_MULTIPLIER: u64 = 5;
    /// Threshold for the number of successful chunks to restore the block range to the default value.
    pub const SUCCESSFUL_CHUNKS_THRESHOLD: u8 = 32;
}

/// Error codes that are used when calling exit.
pub mod error_codes {
    /// The DB file is in use by another process.
    pub const ERROR_CODE_DATABASE_LOCKED: i32 = 2;
    /// Bad DB state.
    pub const ERROR_CODE_BAD_DB_STATE: i32 = 3;
    /// Wrong input arguments.
    pub const ERROR_CODE_WRONG_INPUT_ARGUMENTS: i32 = 4;
    /// Indexing failed.
    pub const ERROR_CODE_INDEXING_FAILED: i32 = 5;
    /// Configuration file not found.
    pub const ERROR_CODE_CONFIGURATION_FILE_NOT_FOUND: i32 = 6;
    /// Failed to load configuration from file.
    pub const ERROR_CODE_FAILED_TO_LOAD_CONFIGURATION_FROM_FILE: i32 = 7;
    /// RPC server is syncing.
    pub const ERROR_CODE_RPC_SERVER_SYNCING: i32 = 8;
}

/// Module with definitions related to the storage of the indexed data.
pub mod storage {
    pub mod storage_api;
    pub use storage_api::{Storage, StorageFactory};

    #[cfg(feature = "duckdb")]
    pub mod storage_duckdb;
    #[cfg(feature = "duckdb")]
    pub use storage_duckdb::DuckDBStorage;

    #[cfg(feature = "postgresql")]
    pub mod storage_postgresql;
    #[cfg(feature = "postgresql")]
    pub use storage_postgresql::PostgreSqlStorage;

    use serde::Serialize;

    // Objects for the REST API.

    /// Data object that represents an event descriptor in the database.
    #[derive(Debug, Clone, Serialize, Default)]
    #[serde(rename_all = "camelCase")]
    pub struct EventDescriptorDb {
        pub chain_id: Option<u64>,
        pub event_hash: Option<String>,
        pub event_signature: Option<String>,
        pub event_name: Option<String>,
        pub first_block: Option<u64>,
        pub last_block: Option<u64>,
        pub event_count: Option<usize>,
    }

    /// Data object that represents a contract descriptor in the database.
    #[derive(Debug, Clone, Serialize)]
    pub struct ContractDescriptorDb {
        pub contract_address: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub contract_name: Option<String>,
    }
}

/// Object that represents a chunk of logs coming from the eth_getLogs call.
pub struct LogChunk {
    pub start_block: u64,
    pub end_block: u64,
    pub events: Vec<alloy::rpc::types::Log>,
}

pub type TxLogChunk = tokio::sync::mpsc::Sender<LogChunk>;
pub type RxLogChunk = tokio::sync::mpsc::Receiver<LogChunk>;
pub type RxCancellationToken = tokio::sync::broadcast::Receiver<()>;

/// Cancellation token for a graceful shutdown of the components of the indexer app.
#[derive(Clone)]
pub struct CancellationToken(tokio::sync::broadcast::Sender<()>);

impl Default for CancellationToken {
    fn default() -> Self {
        Self(tokio::sync::broadcast::Sender::new(1))
    }
}

impl CancellationToken {
    pub fn subscribe(&self) -> RxCancellationToken {
        self.0.subscribe()
    }

    pub fn graceful_shutdown(&self) {
        self.0.send(()).unwrap();
    }
}

/// Data object that represents the status of an event in the database.
#[derive(Debug, Clone)]
pub struct EventStatus {
    pub hash: String,
    pub first_block: u64,
    pub last_block: u64,
    pub event_count: usize,
}

/// Extension trait for displaying `Option<Address>` with a fallback.
pub trait OptionalAddressDisplay {
    fn display_or_none(&self) -> String;
}

impl OptionalAddressDisplay for Option<alloy::primitives::Address> {
    fn display_or_none(&self) -> String {
        match self {
            Some(addr) => addr.to_string(),
            None => "None".to_string(),
        }
    }
}

#[cfg(feature = "test-utils")]
pub mod test_utils;

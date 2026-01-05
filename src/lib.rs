// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

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
pub mod streamlit_wrapper;
pub use configuration::IndexerConfiguration;

use alloy::transports::http::reqwest::Url;
use anyhow::Result;
use secrecy::{ExposeSecret, SecretString};
use std::string::ToString;

/// Module with constants used throughout the application.
pub mod constants {
    /// Enables back pressure for the indexing buffer, as producers might overwhelm the buffer when the RPC server is powerful.
    pub const DEFAULT_INDEXING_BUFFER: usize = 10;
    /// Base address for the API server that runs locally.
    pub const DEFAULT_API_SERVER_ADDRESS: &str = "127.0.0.1:9720";
    /// Default poll interval in seconds when indexing the latest block.
    pub const DEFAULT_POLL_INTERVAL: u64 = 1;

    /// Default block range for the event collector. This range is used in the get_Logs call.
    pub const DEFAULT_BLOCK_RANGE: usize = 10000;
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
    /// Value to choose between using filters or not on eth_getLogs.
    pub const DEFAULT_USE_FILTERS_THRESHOLD: usize = 3;
    /// Reduced block range threshold: multiply the default block range by this value to get the threshold.
    pub const DEFAULT_REDUCED_BLOCK_RANGE_THRESHOLD: usize = 2;
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
}

/// Module with definitions related to the storage of the indexed data.
pub mod storage {
    pub mod storage_api;
    pub use storage_api::Storage;
    pub mod storage_duckdb;
    pub use storage_duckdb::{DuckDBStorage, DuckDBStorageFactory};

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

/// Object that represents an RPC host.
#[derive(Debug, Clone)]
pub struct RpcHost {
    pub url: String,
    pub port: u16,
    pub username: Option<SecretString>,
    pub password: Option<SecretString>,
}

impl std::str::FromStr for RpcHost {
    type Err = anyhow::Error;

    /// Parses the RPC host URL and returns a RpcHost struct.
    ///
    /// # Description
    ///
    /// The format of the RPC host URL is: [<username>:<password>@]<url>
    /// where <url> can be either:
    /// - A full URL: http://host:port/path or https://host:port/path
    /// - A simple host:port format: host:port
    ///
    /// Examples:
    /// - With auth: user:pass@http://localhost:8545
    /// - Without auth: http://localhost:8545
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // Check if there's an @ symbol (indicating credentials are present)
        let (creds_part, url_part) = if let Some(at_pos) = input.rfind('@') {
            // Split at the last @ to handle URLs that might contain @ in other places
            let creds = &input[..at_pos];
            let url = &input[at_pos + 1..];
            (Some(creds), url)
        } else {
            // No credentials, the entire string is the URL
            (None, input)
        };

        // Check if the URL part is already a full URL (starts with http:// or https://)
        let (url, port) = if url_part.starts_with("http://") || url_part.starts_with("https://") {
            // Parse the full URL to extract port for the struct
            let parsed_url = Url::parse(url_part)
                .map_err(|e| anyhow::anyhow!("Failed to parse URL '{}': {}", url_part, e))?;

            let port = parsed_url.port().unwrap_or_else(|| {
                if parsed_url.scheme() == "https" {
                    443
                } else {
                    80
                }
            });

            // Store the full URL as-is (including path if present)
            (url_part.to_string(), port)
        } else {
            // Handle simple host:port format
            let raw_url = url_part.split(':').collect::<Vec<&str>>();

            let port = if raw_url.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid URL format. Expected host:port or full URL, got: {}",
                    url_part
                ));
            } else {
                raw_url[1].parse::<u16>().map_err(|e| {
                    anyhow::anyhow!("Invalid port number in URL '{}': {}", url_part, e)
                })?
            };

            (raw_url[0].to_string(), port)
        };

        // Parse credentials if present
        let (username, password) = if let Some(creds) = creds_part {
            let cred_parts = creds.split(':').collect::<Vec<&str>>();
            if cred_parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid credentials format. Expected username:password, got: {}",
                    creds
                ));
            }
            (
                Some(SecretString::from(cred_parts[0])),
                Some(SecretString::from(cred_parts[1])),
            )
        } else {
            (None, None)
        };

        Ok(RpcHost {
            url,
            port,
            username,
            password,
        })
    }
}

impl TryInto<Url> for &RpcHost {
    type Error = anyhow::Error;
    fn try_into(self) -> Result<Url> {
        // Check if the URL already contains a scheme (http:// or https://)
        let url_str = if self.url.starts_with("http://") || self.url.starts_with("https://") {
            // It's already a full URL, use it as-is
            self.url.clone()
        } else {
            // It's a simple host, construct the URL with the port
            format!("http://{}:{}", self.url, self.port)
        };

        let mut url = Url::parse(&url_str)
            .map_err(|e| anyhow::anyhow!("Failed to create URL from '{}': {}", url_str, e))?;

        if let Some(username) = &self.username {
            url.set_username(username.expose_secret())
                .map_err(|e| anyhow::anyhow!("Failed to set username: {e:?}"))?;
        }
        if let Some(password) = &self.password {
            url.set_password(Some(password.expose_secret()))
                .map_err(|e| anyhow::anyhow!("Failed to set password: {e:?}"))?;
        }

        Ok(url)
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

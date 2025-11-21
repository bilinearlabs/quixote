// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Library of the Etherduck crate.

pub mod event_collector_runner;
pub use event_collector_runner::EventCollectorRunner;
pub mod event_collector;
pub mod event_processor;
pub use event_processor::EventProcessor;
pub mod api_rest;
pub mod cli;
pub mod indexing_app;
pub use indexing_app::IndexingApp;

use alloy::transports::http::reqwest::Url;
use anyhow::Result;
use secrecy::{ExposeSecret, SecretString};
use std::string::ToString;
use strum_macros::Display;

/// Module with constants used throughout the application.
pub mod constants {
    /// Enables back pressure for the indexing buffer, as producers might overwhelm the buffer when the RPC server is powerful.
    pub const DEFAULT_INDEXING_BUFFER: usize = 10;
    /// Base address for the API server that runs locally.
    pub const DEFAULT_API_SERVER_ADDRESS: &str = "127.0.0.1:9720";
    /// Default poll interval in seconds when indexing the latest block.
    pub const DEFAULT_POLL_INTERVAL: u64 = 1;

    /// Default block range for the event collector. This range is used in the get_Logs call.
    pub const DEFAULT_BLOCK_RANGE: usize = 10;
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
    pub const DUCKDB_FILE_PATH: &str = "etherduck_indexer.duckdb";
    /// Schema version for the DuckDB database.
    pub const DUCKDB_SCHEMA_VERSION: &str = "0.1.0";
    /// Base table name for the DuckDB database.
    pub const DUCKDB_BASE_TABLE_NAME: &str = "etherduck_info";
}

/// Module with definitions related to the storage of the indexed data.
pub mod storage {
    pub mod storage;
    pub use storage::Storage;
    pub mod storage_duckdb;
    pub mod storage_query;
    pub use storage_duckdb::{DuckDBStorage, DuckDBStorageFactory};
    pub use storage_query::StorageQuery;

    use alloy::primitives::Address;
    pub use chrono::{DateTime, Utc};
    use serde::{Serialize, Serializer};

    // Objects for the REST API.

    /// Data object that represents an event descriptor in the database.
    #[derive(Debug, Clone, Serialize)]
    pub struct EventDescriptorDb {
        pub event_hash: String,
        pub event_signature: String,
        pub event_name: String,
    }

    /// Data object that represents an event in the database.
    ///
    /// TODO: Update the fields to include the non-indexed parameters.
    #[derive(Debug, Clone, Serialize)]
    pub struct EventDb {
        pub block_number: u64,
        pub transaction_hash: String,
        pub log_index: u64,
        pub contract_address: Address,
        pub topic0: String,
        pub topic1: Option<String>,
        pub topic2: Option<String>,
        pub topic3: Option<String>,
        #[serde(serialize_with = "serialize_timestamp")]
        pub block_timestamp: u64,
    }

    /// Serializes a timestamp to an ISO 8601 string.
    fn serialize_timestamp<S>(timestamp: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as ISO 8601 string
        let dt = DateTime::<Utc>::from_timestamp(*timestamp as i64, 0)
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
        serializer.serialize_str(&dt.to_rfc3339())
    }

    /// Data object that represents a contract descriptor in the database.
    #[derive(Debug, Clone, Serialize)]
    pub struct ContractDescriptorDb {
        pub contract_address: String,
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

impl CancellationToken {
    pub fn new() -> Self {
        Self(tokio::sync::broadcast::Sender::new(1))
    }

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
    pub chain_id: u64,
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
    /// The format of the RPC host URL is: <chain_id>[:<username>:<password>@]<url>
    /// where <url> can be either:
    /// - A full URL: http://host:port/path or https://host:port/path
    /// - A simple host:port format: host:port
    fn from_str(url: &str) -> Result<Self, Self::Err> {
        // Let's break down the input string in 2 parts: the initial data and the URL.
        let parts = url.split('@').collect::<Vec<&str>>();

        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid RPC host URL: {}", url));
        }

        let url_part = parts[1];

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

        // Time to process the initial data part.
        let init_part = parts[0].split(':').collect::<Vec<&str>>();
        let chain_id = init_part[0].parse::<u64>()?;

        let (username, password) = if init_part.len() > 1 {
            (
                Some(SecretString::from(init_part[1])),
                Some(SecretString::from(init_part[2])),
            )
        } else {
            (None, None)
        };

        Ok(RpcHost {
            chain_id,
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

/// Collector running mode
///
/// # Description
///
/// The collector runner can run in two modes:
/// - Multiple event with filtering: Several events are provided using the -e flag.
/// - Multiple event without filtering: The events are provided using the -a flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Display)]
pub enum CollectorRunningMode {
    #[default]
    #[strum(to_string = "Multiple events with filtering")]
    EventWithFiltering,
    #[strum(to_string = "Multiple events without filtering")]
    EventWithoutFiltering,
}

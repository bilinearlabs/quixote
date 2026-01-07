// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the configuration of the application.

use crate::{cli::IndexingArgs, constants, error_codes};
use clap::Parser;
use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, error};

/// Configuration as parsed from a file. Fields are optional to allow partial configs.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FileConfiguration {
    #[serde(default)]
    pub index_jobs: Vec<IndexJob>,
    pub database_path: Option<String>,
    pub api_server_address: Option<String>,
    pub api_server_port: Option<u16>,
    pub frontend_address: Option<String>,
    pub frontend_port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndexJob {
    pub rpc_url: String,
    pub contract: String,
    pub start_block: Option<u64>,
    pub block_range: Option<usize>,
    pub events: Option<Vec<EventJob>>,
    pub abi_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventJob {
    pub full_signature: String,
    pub filters: Option<HashMap<String, String>>,
}

/// Fully resolved configuration with all defaults applied.
#[derive(Debug, Clone)]
pub struct IndexerConfiguration {
    pub index_jobs: Vec<IndexJob>,
    pub database_path: String,
    pub api_server_address: String,
    pub api_server_port: u16,
    pub frontend_address: String,
    pub frontend_port: u16,
    pub verbosity: u8,
    pub disable_frontend: bool,
    pub strict_mode: bool,
}

impl IndexerConfiguration {
    /// Build the indexer configuration from CLI arguments.
    ///
    /// If a config file is provided, it takes precedence for all file-based fields.
    /// CLI arguments for those fields are ignored when a config file is used.
    /// CLI-only options (`verbosity`, `disable_frontend`, `strict_mode`) are always taken
    /// from the command line.
    ///
    /// # Panics
    ///
    /// This function will log an error and exit the process if the configuration file
    /// cannot be found or contains parsing errors.
    pub fn from_args(args: IndexingArgs) -> Self {
        // If a config file is provided, it takes precedence for file-based fields.
        let file_config = if let Some(ref config_file) = args.config {
            debug!("Loading configuration from file: {}", config_file);
            debug!("CLI arguments for file-based fields will be ignored");
            match FileConfiguration::load(config_file) {
                Ok(config) => config,
                Err(e) => match e {
                    ConfigError::NotFound(ref path) => {
                        error!("Configuration file not found: {}", path);
                        std::process::exit(error_codes::ERROR_CODE_CONFIGURATION_FILE_NOT_FOUND);
                    }
                    ConfigError::FileParse { ref uri, ref cause } => {
                        error!(
                            "Failed to parse configuration file: {}",
                            uri.as_deref().unwrap_or(config_file)
                        );
                        error!("Parse error: {}", cause);
                        std::process::exit(
                            error_codes::ERROR_CODE_FAILED_TO_LOAD_CONFIGURATION_FROM_FILE,
                        );
                    }
                    _ => {
                        error!(
                            "Failed to load configuration from file '{}': {}",
                            config_file, e
                        );
                        std::process::exit(
                            error_codes::ERROR_CODE_FAILED_TO_LOAD_CONFIGURATION_FROM_FILE,
                        );
                    }
                },
            }
        } else {
            // No config file, build from CLI arguments
            FileConfiguration::from_args(&args)
        };

        // Resolve all fields with defaults
        Self {
            index_jobs: file_config.index_jobs,
            database_path: file_config
                .database_path
                .unwrap_or_else(|| constants::DUCKDB_FILE_PATH.to_string()),
            api_server_address: file_config
                .api_server_address
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            api_server_port: file_config.api_server_port.unwrap_or(9720),
            frontend_address: file_config
                .frontend_address
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            frontend_port: file_config.frontend_port.unwrap_or(8501),
            verbosity: args.verbosity,
            disable_frontend: args.disable_frontend,
            strict_mode: args.strict_mode,
        }
    }

    /// Parse CLI arguments and build the indexer configuration.
    ///
    /// # Panics
    ///
    /// This function will log an error and exit the process if the configuration file
    /// cannot be found or contains parsing errors.
    pub fn parse() -> Self {
        let args = IndexingArgs::parse();
        Self::from_args(args)
    }
}

impl FileConfiguration {
    /// Build from CLI arguments (no config file).
    ///
    /// # Panics
    ///
    /// This function will log an error and exit if the RPC host string cannot be parsed.
    pub fn from_args(args: &IndexingArgs) -> Self {
        let index_jobs = if let Some(ref contract) = args.contract {
            let events = args.event.as_ref().map(|evts| {
                evts.iter()
                    .map(|sig| EventJob {
                        full_signature: sig.clone(),
                        filters: None,
                    })
                    .collect()
            });

            // Use the RPC URL directly (standard URL format expected)
            let rpc_url = args.rpc_host.clone().unwrap_or_default();

            vec![IndexJob {
                rpc_url,
                contract: contract.clone(),
                start_block: args.start_block.as_ref().and_then(|s| s.parse().ok()),
                block_range: Some(args.block_range),
                events,
                abi_path: args.abi_spec.clone(),
            }]
        } else {
            vec![]
        };

        Self {
            index_jobs,
            database_path: args.database.clone(),
            api_server_address: args
                .api_server
                .as_ref()
                .and_then(|s| s.split(':').next())
                .map(|s| s.to_string()),
            api_server_port: args
                .api_server
                .as_ref()
                .and_then(|s| s.split(':').nth(1))
                .and_then(|p| p.parse().ok()),
            frontend_address: Some(args.frontend_address.clone()),
            frontend_port: Some(args.frontend_port),
        }
    }

    /// Load from a YAML/JSON file.
    pub fn load(config_file: &str) -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::from(Path::new(config_file)))
            .add_source(Environment::with_prefix("QUIXOTE"))
            .build()?
            .try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::IndexingArgs;
    use crate::constants::DEFAULT_BLOCK_RANGE;
    use rstest::rstest;

    const TEST_CONTRACT: &str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";

    /// Helper to create IndexingArgs for testing
    fn create_test_args(rpc_host: Option<String>, contract: Option<String>) -> IndexingArgs {
        IndexingArgs {
            rpc_host,
            contract,
            event: None,
            start_block: None,
            database: None,
            abi_spec: None,
            api_server: None,
            block_range: DEFAULT_BLOCK_RANGE,
            verbosity: 1,
            disable_frontend: false,
            frontend_address: "127.0.0.1".to_string(),
            frontend_port: 8501,
            strict_mode: false,
            config: None,
        }
    }

    #[rstest]
    #[case::with_credentials("http://user:pass@localhost:8545")]
    #[case::without_credentials("http://localhost:8545")]
    #[case::https_url("https://eth.example.com")]
    #[case::https_with_credentials("https://user:pass@eth.example.com")]
    fn from_args_uses_rpc_url_directly(#[case] input: &str) {
        let args = create_test_args(Some(input.to_string()), Some(TEST_CONTRACT.to_string()));

        let config = FileConfiguration::from_args(&args);

        assert_eq!(config.index_jobs.len(), 1);
        // URL is used directly without transformation
        assert_eq!(config.index_jobs[0].rpc_url, input);
    }

    #[rstest]
    fn from_args_without_contract_creates_no_jobs() {
        let args = create_test_args(Some("http://localhost:8545".to_string()), None);

        let config = FileConfiguration::from_args(&args);

        assert!(config.index_jobs.is_empty());
    }

    #[rstest]
    fn from_args_with_events_creates_event_jobs() {
        let mut args = create_test_args(
            Some("http://localhost:8545".to_string()),
            Some(TEST_CONTRACT.to_string()),
        );
        args.event = Some(vec![
            "Transfer(address indexed from, address indexed to, uint256 value)".to_string(),
            "Approval(address indexed owner, address indexed spender, uint256 value)".to_string(),
        ]);

        let config = FileConfiguration::from_args(&args);

        assert_eq!(config.index_jobs.len(), 1);
        let events = config.index_jobs[0].events.as_ref().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events[0].full_signature.contains("Transfer"));
        assert!(events[1].full_signature.contains("Approval"));
    }
}

// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Runner module for the event collector.

use crate::{CollectorSeed, RpcHost, TxLogChunk, constants::*, event_collector::EventCollector};
use alloy::{
    providers::Provider,
    providers::ProviderBuilder,
    rpc::client::RpcClient,
    transports::{
        TransportError,
        http::reqwest::Url,
        layers::{RetryBackoffLayer, RetryPolicy},
    },
};
use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};

#[derive(Debug, Copy, Clone, Default)]
#[non_exhaustive]
pub struct AlwaysRetryPolicy;

impl RetryPolicy for AlwaysRetryPolicy {
    fn should_retry(&self, _error: &TransportError) -> bool {
        // TODO: Be more granular with the retry policy.
        // we don't want to retry in some cases.
        true
    }

    fn backoff_hint(&self, _error: &TransportError) -> Option<std::time::Duration> {
        None
    }
}

/// Runner for the event collector.
///
/// # Description
///
/// This runner is responsible for spawning the event collector tasks for each RPC host. Each RPC host can serve
/// data from a different blockchain. This way, we can monitor some event across multiple chains. However, this is
/// not fully supported yet.
///
/// ## Resource assignment
///
/// The best case scenario would be to assign one seed per RPC host. However having multiple RPC hosts is not always
/// possible. Each seed is assigned to a producer task, this way, all the events that belong to the same seed are
/// ensured to be synchronized up to the same block.
pub struct EventCollectorRunner {
    provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>>,
    seeds: Vec<CollectorSeed>,
    producer_buffer: TxLogChunk,
    default_block_range: usize,
}

impl EventCollectorRunner {
    pub fn new(
        host_list: &[RpcHost],
        seeds: Vec<CollectorSeed>,
        producer_buffer: TxLogChunk,
        default_block_range: usize,
    ) -> Result<Self> {
        let always_retry_policy = AlwaysRetryPolicy::default();

        let retry_policy = RetryBackoffLayer::new_with_policy(
            DEFAULT_BACKOFF_LAYER_MAX_RETRIES,
            DEFAULT_BACKOFF_LAYER_BACKOFF_TIME,
            DEFAULT_BACKOFF_LAYER_CUP_SIZE,
            always_retry_policy,
        );

        let mut provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>> = Vec::new();
        for (idx, host) in host_list.iter().enumerate() {
            let url: Url = host
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to convert RPC host to URL: {}", e))?;

            info!(
                "Creating provider {} for RPC URL: {} (chain_id: {})",
                idx, url, host.chain_id
            );

            let provider = ProviderBuilder::new()
                .connect_client(RpcClient::builder().layer(retry_policy.clone()).http(url));
            provider_list.push(Arc::new(provider));
        }

        Ok(Self {
            provider_list,
            seeds,
            producer_buffer,
            default_block_range,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting the event collector runner");

        if self.provider_list.is_empty() {
            error!("No providers available");
            anyhow::bail!("Exiting app as no providers are available");
        }

        info!(
            "Distributing {} seeds across {} RPC hosts",
            self.seeds.len(),
            self.provider_list.len()
        );

        // Collect all task handles
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Distribute seeds across RPC hosts using round-robin
        // If there are more seeds than RPC hosts, multiple seeds will use the same RPC host
        for (seed_index, seed) in self.seeds.iter().enumerate() {
            // Assign seed to RPC host using round-robin
            let host_index = seed_index % self.provider_list.len();
            let provider = self.provider_list[host_index].clone();
            let producer_buffer = self.producer_buffer.clone();
            let seed = seed.clone();
            let default_block_range = self.default_block_range;

            info!(
                "Spawning collector for seed {} (contract: {}, start_block: {}) on RPC host {}",
                seed_index, seed.contract_address, seed.start_block, host_index
            );

            let handle = tokio::spawn(async move {
                let collector =
                    EventCollector::new(provider, producer_buffer, &seed, default_block_range);

                if let Err(e) = collector.collect().await {
                    error!(
                        "Collector for seed {} (contract: {}) failed: {}",
                        seed_index, seed.contract_address, e
                    );
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete (they run forever, so this will only return on error)
        for handle in handles {
            handle.await?;
        }

        info!("All collector tasks completed");
        Ok(())
    }
}

// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Runner module for the event collector.

use crate::{
    CollectorSeed, TxLogChunk, constants::*, event_collector::EventCollector,
    metrics::MetricsHandle,
};
use alloy::{
    providers::Provider,
    providers::ProviderBuilder,
    rpc::client::RpcClient,
    transports::{
        TransportError,
        layers::{RetryBackoffLayer, RetryPolicy},
    },
};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

#[derive(Debug, Copy, Clone, Default)]
#[non_exhaustive]
pub struct AlwaysRetryPolicy;

impl RetryPolicy for AlwaysRetryPolicy {
    fn should_retry(&self, error: &TransportError) -> bool {
        // Convert error to string to check for specific error codes
        let error_msg = format!("{}", error);
        let error_lower = error_msg.to_lowercase();

        // Check if this is the "query exceeds max results" error (code -32602)
        // This error should NOT be retried as it indicates the query range is too large
        // Check for error code -32602 (case-insensitive) and the specific message
        if (error_lower.contains("-32602") || error_lower.contains("error code -32602"))
            && error_lower.contains("query exceeds max results")
        {
            debug!(
                "Not retrying request due to query exceeds max results error: {}",
                error_msg
            );
            return false;
        }

        // By default, retry all other errors
        true
    }

    fn backoff_hint(&self, error: &TransportError) -> Option<Duration> {
        // Convert error to string to check for backoff hints
        let error_msg = format!("{}", error);

        // Check for common rate limit patterns in the error message
        // Look for patterns like "retry after", "wait", "rate limit", etc.
        let error_lower = error_msg.to_lowercase();

        // Check for "retry after X seconds" or similar patterns
        if let Some(seconds) = extract_retry_after_seconds(&error_lower) {
            debug!(
                "Extracted backoff hint from error: {} seconds (error: {})",
                seconds, error_msg
            );
            return Some(Duration::from_secs(seconds));
        }

        // Check for "wait X seconds" pattern
        if let Some(seconds) = extract_wait_seconds(&error_lower) {
            debug!(
                "Extracted wait hint from error: {} seconds (error: {})",
                seconds, error_msg
            );
            return Some(Duration::from_secs(seconds));
        }

        // Check for rate limit indicators (common patterns)
        if error_lower.contains("rate limit") || error_lower.contains("too many requests") {
            debug!(
                "Rate limit detected in error, using default backoff hint (error: {})",
                error_msg
            );
            // Return a reasonable default backoff for rate limits (e.g., 1 second)
            // The actual backoff will be handled by RetryBackoffLayer
            return Some(Duration::from_secs(1));
        }

        None
    }
}

/// Extract retry after seconds from error message
/// Looks for patterns like "retry after 5 seconds", "retry after 5s", "retry-after: 5", etc.
fn extract_retry_after_seconds(error_msg: &str) -> Option<u64> {
    // Try to find "retry after" or "retry-after" pattern
    let patterns = ["retry after", "retry-after"];
    for pattern in patterns.iter() {
        if let Some(pos) = error_msg.find(pattern) {
            let after_pos = pos + pattern.len();
            let remaining = &error_msg[after_pos..];

            // Skip whitespace and common separators
            let remaining = remaining.trim_start_matches(|c: char| c.is_whitespace() || c == ':');

            // Find the first number in the remaining string
            let num_start = remaining.find(|c: char| c.is_ascii_digit())?;
            let num_part = &remaining[num_start..];

            // Extract the number (stop at first non-digit)
            let num_end = num_part
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(num_part.len());
            let num_str = &num_part[..num_end];

            if let Ok(num) = num_str.parse::<u64>() {
                // Check if followed by time unit indicators (second, seconds, s, sec, etc.)
                let after_num = &num_part[num_end..].trim_start();
                if after_num.is_empty()
                    || after_num.starts_with("second")
                    || after_num.starts_with("s")
                    || after_num.starts_with("sec")
                {
                    return Some(num);
                }
            }
        }
    }
    None
}

/// Extract wait seconds from error message
/// Looks for patterns like "wait 5 seconds", "wait 5s", etc.
fn extract_wait_seconds(error_msg: &str) -> Option<u64> {
    // Try to find "wait" pattern
    if let Some(pos) = error_msg.find("wait") {
        let after_pos = pos + "wait".len();
        let remaining = &error_msg[after_pos..];

        // Skip whitespace
        let remaining = remaining.trim_start();

        // Find the first number in the remaining string
        let num_start = remaining.find(|c: char| c.is_ascii_digit())?;
        let num_part = &remaining[num_start..];

        // Extract the number (stop at first non-digit)
        let num_end = num_part
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(num_part.len());
        let num_str = &num_part[..num_end];

        if let Ok(num) = num_str.parse::<u64>() {
            // Check if followed by time unit indicators
            let after_num = &num_part[num_end..].trim_start();
            if after_num.is_empty()
                || after_num.starts_with("second")
                || after_num.starts_with("s")
                || after_num.starts_with("sec")
            {
                return Some(num);
            }
        }
    }
    None
}

/// Runner for the event collector.
///
/// # Description
///
/// This runner is responsible for spawning the event collector tasks for each seed. Each seed contains
/// its own RPC URL and chain_id, allowing different seeds to connect to different chains.
///
/// ## Resource assignment
///
/// Each seed is assigned to its own provider, producer task, and buffer, ensuring complete isolation.
/// Each seed is an independent unit of work with its own collector.
pub struct EventCollectorRunner {
    /// List of providers, one per seed, created from each seed's rpc_url.
    provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>>,
    seeds: Vec<CollectorSeed>,
    /// Per-seed buffers: one TxLogChunk per seed (same order as seeds)
    seed_buffers: Vec<TxLogChunk>,
    metrics: MetricsHandle,
}

impl EventCollectorRunner {
    pub fn new(
        seeds: Vec<CollectorSeed>,
        seed_buffers: Vec<TxLogChunk>,
        metrics: MetricsHandle,
    ) -> Result<Self> {
        if seeds.len() != seed_buffers.len() {
            anyhow::bail!(
                "Mismatch: {} seeds but {} buffers provided",
                seeds.len(),
                seed_buffers.len()
            );
        }

        let always_retry_policy = AlwaysRetryPolicy::default();

        let retry_policy = RetryBackoffLayer::new_with_policy(
            DEFAULT_BACKOFF_LAYER_MAX_RETRIES,
            DEFAULT_BACKOFF_LAYER_BACKOFF_TIME,
            DEFAULT_BACKOFF_LAYER_CUP_SIZE,
            always_retry_policy,
        );

        // Create a provider for each seed from its rpc_url (already validated)
        let mut provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>> = Vec::new();
        for (idx, seed) in seeds.iter().enumerate() {
            info!(
                "Creating provider {} for RPC URL: {} (chain_id: {:#x})",
                idx, seed.rpc_url, seed.chain_id
            );

            let provider = ProviderBuilder::new().connect_client(
                RpcClient::builder()
                    .layer(retry_policy.clone())
                    .http(seed.rpc_url.clone()),
            );
            provider_list.push(Arc::new(provider));
        }

        Ok(Self {
            provider_list,
            seeds,
            seed_buffers,
            metrics,
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting the event collector runner");

        if self.provider_list.is_empty() {
            error!("No providers available");
            anyhow::bail!("Exiting app as no providers are available");
        }

        info!("Starting {} collector task(s)", self.seeds.len());

        // Collect all task handles
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Each seed has its own provider and buffer (1:1:1 mapping)
        for (seed_index, ((seed, provider), buffer)) in self
            .seeds
            .iter()
            .zip(self.provider_list.iter())
            .zip(self.seed_buffers.iter())
            .enumerate()
        {
            let provider = provider.clone();
            let producer_buffer = buffer.clone();
            let seed = seed.clone();

            info!(
                "Spawning collector {} (chain_id: {:#x}, contract: {}, start_block: {}, block_range: {})",
                seed_index,
                seed.chain_id,
                seed.contract_address,
                seed.start_block,
                seed.block_range
            );

            let metrics = self.metrics.clone();
            let handle = tokio::spawn(async move {
                let collector = EventCollector::new(provider, producer_buffer, &seed, metrics);

                if let Err(e) = collector.collect().await {
                    if e.to_string().contains("channel closed") {
                        info!(
                            "Collector {} (chain_id: {:#x}, contract: {}) channel closed",
                            seed_index, seed.chain_id, seed.contract_address
                        );
                    } else {
                        error!(
                            "Collector {} (chain_id: {:#x}, contract: {}) failed: {}",
                            seed_index, seed.chain_id, seed.contract_address, e
                        );
                    }
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

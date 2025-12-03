// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event collector.

use crate::{CollectorSeed, LogChunk, TxLogChunk, constants::*};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::Provider,
    rpc::types::{Filter, SyncStatus},
};
use anyhow::Result;
use futures::stream::{self, TryStreamExt};
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct EventCollector {
    contract_address: Address,
    filter: Option<Filter>,
    start_block: u64,
    provider: Arc<dyn Provider + Send + Sync>,
    sync_mode: BlockNumberOrTag,
    poll_interval: u64,
    producer_buffer: TxLogChunk,
    default_block_range: usize,
    block_range_hint_regex: regex::Regex,
}

impl EventCollector {
    pub fn new(
        provider: Arc<dyn Provider + Send + Sync>,
        producer_buffer: TxLogChunk,
        seed: &CollectorSeed,
        default_block_range: usize,
    ) -> Self {
        // Regex to capture the last two integers (block numbers) from messages like:
        // "error code -32602: query exceeds max results 20000, retry with the range 22382105-22382515"
        let block_range_hint_regex = regex::Regex::new(r"(\d+)-(\d+)\s*$").unwrap();
        Self {
            contract_address: seed.contract_address,
            filter: seed.filter.clone(),
            start_block: seed.start_block,
            provider,
            sync_mode: seed.sync_mode,
            poll_interval: DEFAULT_POLL_INTERVAL,
            producer_buffer,
            default_block_range,
            block_range_hint_regex,
        }
    }

    pub async fn collect(&self) -> Result<()> {
        if self.check_sync_status().await? {
            error!(
                "The RPC server is syncing, resume indexing when the syncing process is complete"
            );
            std::process::exit(1);
        }

        let mut processed_to = self.start_block.saturating_sub(1);
        let mut block_range = self.default_block_range;
        // Wrapped variables: whe the value is None, it means no errors happened in the previous iteration, thus we
        // can initialize the variables as usual. When the value is Some, it means an error happened in the previous
        //iteration, and we need to reuse the values from the failed iteration.
        let mut chunk_starts: Option<Vec<u64>> = None;
        let mut finalized_block: Option<u64> = None;
        // Track how many blocks have been successfully processed with the current (reduced) block_range.
        let mut blocks_processed_with_current_range: u64 = 0;
        // Threshold: after processing this many blocks successfully, try to increase block_range.
        let block_range_increase_threshold =
            (self.default_block_range * DEFAULT_REDUCED_BLOCK_RANGE_THRESHOLD) as u64;

        loop {
            if chunk_starts.is_none() {
                let provider = self.provider.clone();
                let current_finalized_block =
                    match provider.get_block_by_number(self.sync_mode).await {
                        Ok(Some(block)) => block.header.number,
                        Ok(None) => return Err(anyhow::anyhow!("Finalized block is None")),
                        Err(e) => {
                            error!("Failed to get finalized block from RPC provider: {}", e);
                            return Err(anyhow::anyhow!("RPC connection error: {}", e));
                        }
                    };

                let remaining = current_finalized_block.saturating_sub(processed_to);
                if remaining == 0 {
                    sleep(Duration::from_secs(self.poll_interval)).await;
                    continue;
                }

                finalized_block = Some(current_finalized_block);
                chunk_starts = Some(
                    ((processed_to + 1)..=current_finalized_block)
                        .step_by(self.default_block_range)
                        .collect(),
                );
            }

            let chunk_starts_vec = chunk_starts.as_ref().unwrap();
            let current_finalized_block = finalized_block.unwrap();
            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();
            let current_block_range = block_range;

            // Detect errors to reduce the block range if needed.
            if let Err(e) = stream::iter(
                chunk_starts_vec
                    .iter()
                    .copied()
                    .map(Ok::<u64, anyhow::Error>),
            )
            .try_for_each_concurrent(MAX_CONCURRENT_RPC_REQUESTS, |chunk_start| {
                let tx = producer_buffer.clone();
                let provider = provider_clone.clone();
                let block_range = current_block_range;
                let finalized_block = current_finalized_block;

                async move {
                    // Check tha the RPC server is not syncing. If syncing, a raw exit is issued as we prefer to stop
                    // all the running logic just in case some RPC request returns inconsistent data that gets
                    // stored in the database.
                    if self.check_sync_status().await? {
                        error!("The RPC server is syncing, resume indexing when the syncing process is complete");
                        std::process::exit(1);
                    }

                    let chunk_end = std::cmp::min(
                        chunk_start + block_range as u64 - 1,
                        finalized_block,
                    );

                    info!(
                        "Fetching events for blocks [{chunk_start}-{chunk_end}] contract address: {contract_address}"
                    );

                    // Build the base filter for the get_Logs call. By default, all the events for a given smart
                    // contract are fetched.
                    let mut filter = Filter::new()
                        .from_block(chunk_start)
                        .to_block(chunk_end)
                        .address(contract_address);

                    // Add custom filters (topics) if provided.
                    if let Some(custom_filter) = self.filter.clone() && !custom_filter.topics.is_empty() {
                        filter.topics = custom_filter.topics;
                    }

                    let events = provider.get_logs(&filter).await?;

                    tx.send(LogChunk {
                        start_block: chunk_start,
                        end_block: chunk_end,
                        events,
                    })
                    .await?;
                    Ok(())
                }
            })
            .await {
                // If the RPC returns the error: -32602: query exceeds max results 2000, the RPC offers a hint of the
                // valid range, so we take that and lower the indexing for a while.
                let err_msg = e.to_string();
                if err_msg.contains("query exceeds max results") {
                    let new_block_range = if let Some(captures) = self.block_range_hint_regex.captures(&err_msg) {
                        // Extract the two numbers from the regex capture groups
                        if let (Some(first_match), Some(second_match)) = (captures.get(1), captures.get(2)) {
                            let first: u64 = first_match.as_str().parse().unwrap_or(0);
                            let second: u64 = second_match.as_str().parse().unwrap_or(0);
                            let mut range = second.saturating_sub(first);
                            // And apply a safety margin of a 10% of the range.
                            range = range - (range / 10);
                            range as usize
                        } else {
                            block_range >> 1
                        }
                    } else {
                        // If the hint couldn't be extracted, apply a simple reduction logic.
                        block_range >> 1
                    };

                    // Maybe this won't ever happen, but just in case.
                    if new_block_range == 0 {
                        return Err(anyhow::anyhow!(
                            "Block range reduced to zero, cannot continue the indexing."
                        ));
                    }

                    warn!("Throttled RPC server, reducing block range from {block_range} to {new_block_range}");
                    block_range = new_block_range;
                    // Reset the counter when reducing block_range due to error.
                    blocks_processed_with_current_range = 0;
                    // Skip the last variable updates.
                    continue;
                } else {
                    // Any other error shall stop the indexing task.
                    return Err(anyhow::anyhow!("Error received from the RPC server: {e}"));
                }
            }

            // Reached this point no errors happened, so update the counters.
            let blocks_processed_this_iteration =
                current_finalized_block.saturating_sub(processed_to);
            blocks_processed_with_current_range += blocks_processed_this_iteration;

            // Check if we should restore block_range back to default.
            if block_range < self.default_block_range
                && blocks_processed_with_current_range >= block_range_increase_threshold
            {
                info!(
                    "Restoring block range from {block_range} to {} (processed {blocks_processed_with_current_range} blocks successfully with reduced range)",
                    self.default_block_range,
                );
                block_range = self.default_block_range;
                // Reset counter after restoring block_range.
                blocks_processed_with_current_range = 0;
            } else if block_range >= self.default_block_range {
                // Just in case.
                blocks_processed_with_current_range = 0;
            }

            // Clear the retry state and update processed_to.
            chunk_starts = None;
            finalized_block = None;
            processed_to = current_finalized_block;
        }
    }

    async fn check_sync_status(&self) -> Result<bool> {
        let syncing = self.provider.syncing().await?;

        match syncing {
            SyncStatus::Info(_) => Ok(true),
            SyncStatus::None => Ok(false),
        }
    }
}

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
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

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

        // The last stored block number in the DB.
        let mut processed_to = self.start_block.saturating_sub(1);
        // How many blocks per get_logs call.
        let mut chunk_length = self.default_block_range as u64;
        // Counts how many successful chunks have been processed.
        let mut successful_counter: u8 = 0;
        // Flag that indicates whether we are backfilling the database or fetching the finalized block.
        let mut backfill_mode = true;

        loop {
            let provider = self.provider.clone();
            let finalized_block = match provider.get_block_by_number(self.sync_mode).await {
                Ok(Some(block)) => block.header.number,
                Ok(None) => return Err(anyhow::anyhow!("Finalized block is None")),
                Err(e) => {
                    error!("Failed to get finalized block from RPC provider: {}", e);
                    return Err(anyhow::anyhow!("RPC connection error: {}", e));
                }
            };

            let remaining = finalized_block.saturating_sub(processed_to);
            if remaining == 0 {
                // First time, we completed the backfill of the DB, let's inform the user.
                if backfill_mode {
                    info!(
                        "Backfill of the database completed, starting to fetch the finalized block."
                    );
                }
                backfill_mode = false;
                sleep(Duration::from_secs(self.poll_interval)).await;
                continue;
            }

            // Prepare a run of up to 5 times the chunk size.
            let (chunk_starts, step) = if remaining > 5 * chunk_length {
                (
                    ((processed_to + 1)..=processed_to + 5 * chunk_length)
                        .step_by(chunk_length as usize)
                        .collect::<Vec<u64>>(),
                    processed_to + 5 * chunk_length,
                )
            } else {
                (
                    ((processed_to + 1)..=finalized_block)
                        .step_by(chunk_length as usize)
                        .collect::<Vec<u64>>(),
                    finalized_block,
                )
            };

            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();

            // Process the prepared chunks of the chain history concurrently.
            // If an error is detected in the series, all the intermediate results are discarded, and the logic
            // proceeds to reduce the block range and retry the series.
            let rpc_results: Vec<LogChunk> = match stream::iter(
                chunk_starts
                    .into_iter()
            ).map(|chunk_start| {
                let provider = provider_clone.clone();

                async move {
                    // Check tha the RPC server is not syncing. If syncing, a raw exit is issued as we prefer to stop
                    // all the running logic just in case some RPC request returns inconsistent data that gets
                    // stored in the database.
                    if self.check_sync_status().await? {
                        error!("The RPC server is syncing, resume indexing when the syncing process is complete");
                        std::process::exit(1);
                    }

                    let chunk_end = std::cmp::min(
                        chunk_start + chunk_length - 1,
                        finalized_block,
                    );

                    info!(
                        "Fetching events for blocks [{:?}-{:?}] contract address: {:?}",
                        chunk_start, chunk_end, contract_address
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

                    Ok::<_, anyhow::Error>(LogChunk {
                        start_block: chunk_start,
                        end_block: chunk_end,
                        events,
                    })
                }

            })
            .buffer_unordered(MAX_CONCURRENT_RPC_REQUESTS)
            .try_collect()
            .await {
                Ok(results) => results,
                Err(e) => {
                    let err_msg = e.to_string();
                    debug!("Discarded block range: [{processed_to}-{step}]");

                    if err_msg.contains("-32602") {
                        // Did we receive a hint of the valid range?
                        let prev_chunk_length = chunk_length;
                        chunk_length =
                            if let Some(captures) = self.block_range_hint_regex.captures(&err_msg) {
                                // Extract the two numbers from the regex capture groups
                                if let (Some(first_match), Some(second_match)) =
                                    (captures.get(1), captures.get(2)) {
                                    let first: u64 = first_match.as_str().parse().unwrap_or(0);
                                    let second: u64 = second_match.as_str().parse().unwrap_or(0);
                                    let range = second.saturating_sub(first);
                                    // And apply a safety margin of a 10% of the range.
                                    range - (range / 10)
                                } else {
                                    chunk_length >> 1
                                }
                            } else {
                                chunk_length >> 1
                            };

                        if chunk_length == 0 {
                            return Err(anyhow::anyhow!(
                                "Block range reduced to zero, cannot continue the indexing."
                            ));
                        }

                        warn!("Throttled RPC server, reducing block range from {prev_chunk_length} to {chunk_length}");

                        continue;
                    } else {
                        return Err(anyhow::anyhow!("Error received from the RPC server: {e}"));
                    }
                }
            };

            // Reached this statement, we are sure that no errors happened in the series of RPC requests, thus we
            // can send the results to the consumer task for its storage in the DB.
            stream::iter(rpc_results.into_iter().map(Ok::<LogChunk, anyhow::Error>))
                .try_for_each_concurrent(MAX_CONCURRENT_RPC_REQUESTS, |result| {
                    let tx = producer_buffer.clone();
                    async move {
                        tx.send(result).await?;
                        Ok(())
                    }
                })
                .await?;

            // If we reach the threshold of successful chunks, we can restore the block range to the default value.
            if successful_counter == 32 {
                chunk_length = self.default_block_range as u64;
            }

            processed_to = step;
            successful_counter = successful_counter.wrapping_add(1);
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

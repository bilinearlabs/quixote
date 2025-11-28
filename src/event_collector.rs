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
use tracing::{debug, error};

#[derive(Clone)]
pub struct EventCollector {
    contract_address: Address,
    filter: Option<Filter>,
    start_block: u64,
    provider: Arc<dyn Provider + Send + Sync>,
    sync_mode: BlockNumberOrTag,
    poll_interval: u64,
    producer_buffer: TxLogChunk,
}

impl EventCollector {
    pub fn new(
        provider: Arc<dyn Provider + Send + Sync>,
        producer_buffer: TxLogChunk,
        seed: &CollectorSeed,
    ) -> Self {
        Self {
            contract_address: seed.contract_address,
            filter: seed.filter.clone(),
            start_block: seed.start_block,
            provider,
            sync_mode: seed.sync_mode,
            poll_interval: DEFAULT_POLL_INTERVAL,
            producer_buffer,
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
                sleep(Duration::from_secs(self.poll_interval)).await;
                continue;
            }

            let chunk_starts: Vec<u64> = ((processed_to + 1)..=finalized_block)
                .step_by(DEFAULT_BLOCK_RANGE)
                .collect();

            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();
            stream::iter(
                chunk_starts
                    .into_iter()
                    .map(Ok::<u64, anyhow::Error>),
            )
            .try_for_each_concurrent(MAX_CONCURRENT_RPC_REQUESTS, |chunk_start| {
                let tx = producer_buffer.clone();
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
                        chunk_start + DEFAULT_BLOCK_RANGE as u64 - 1,
                        finalized_block,
                    );

                    tracing::info!(
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
                    if let Some(custom_filter) = self.filter.clone() {
                        if !custom_filter.topics.is_empty() {
                            filter.topics = custom_filter.topics;
                        }
                    }

                    debug!("Filter: {:?}", filter);

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
            .await
            .map_err(|e| anyhow::anyhow!("Error collecting events: {}", e))?;

            // All blocks up to `finalized_block` have been queued for processing.
            processed_to = finalized_block;
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

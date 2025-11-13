// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event collector.

use crate::{LogChunk, TxLogChunk};
use alloy::eips::BlockNumberOrTag;
use alloy::primitives::B256;
use alloy::primitives::{Address, keccak256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, FilterSet};
use anyhow::Result;
use futures::stream::{self, TryStreamExt};
use std::error::Error;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

const DEFAULT_POLL_INTERVAL: u64 = 1;

const DEFAULT_BLOCK_RANGE: usize = 100;

const MAX_CONCURRENT_REQUESTS: usize = 4;

#[derive(Clone)]
pub struct EventCollector {
    contract_address: Address,
    events: Vec<String>,
    start_block: u64,
    provider: Arc<dyn Provider + Send + Sync>,
    sync_mode: BlockNumberOrTag,
    poll_interval: u64,
    producer_buffer: TxLogChunk,
}

impl EventCollector {
    pub fn new(
        contract_address: Address,
        events: Vec<String>,
        start_block: u64,
        provider: Arc<dyn Provider + Send + Sync>,
        sync_mode: BlockNumberOrTag,
        producer_buffer: TxLogChunk,
    ) -> Self {
        Self {
            contract_address,
            events,
            start_block,
            provider,
            sync_mode,
            poll_interval: DEFAULT_POLL_INTERVAL,
            producer_buffer,
        }
    }

    pub async fn collect(&self) -> Result<()> {
        let mut processed_to = self.start_block.saturating_sub(1);
        let mut filter_events = FilterSet::default();

        // Hash each event signature and add to the filter
        for event_signature in &self.events {
            let hash = keccak256(event_signature.as_bytes());
            // keccak256 returns [u8; 32], which B256 can be created from
            let event_hash = B256::from(hash);
            filter_events.insert(event_hash);
        }

        println!(
            "Collecting events from blocks [{:?}-{:?}]",
            processed_to, self.sync_mode
        );

        loop {
            let provider = self.provider.clone();
            let finalized_block = provider
                .get_block_by_number(self.sync_mode)
                .await?
                .ok_or(anyhow::anyhow!("Finalized block is None"))?
                .header
                .number;

            let remaining = finalized_block.saturating_sub(processed_to);
            if remaining == 0 {
                sleep(Duration::from_secs(self.poll_interval)).await;
                continue;
            }

            let chunk_starts: Vec<u64> = ((processed_to + 1)..=finalized_block)
                .step_by(DEFAULT_BLOCK_RANGE)
                .collect();

            let filter_events_clone = filter_events.clone();
            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();
            stream::iter(
                chunk_starts
                    .into_iter()
                    .map(Ok::<u64, Box<dyn Error + Send + Sync>>),
            )
            .try_for_each_concurrent(MAX_CONCURRENT_REQUESTS, |chunk_start| {
                let tx = producer_buffer.clone();
                let provider = provider_clone.clone();
                let filter_events = filter_events_clone.clone();
                let contract_address = contract_address;
                async move {
                    let chunk_end = std::cmp::min(
                        chunk_start + DEFAULT_BLOCK_RANGE as u64 - 1,
                        finalized_block,
                    );

                    println!(
                        "Fetching events for blocks [{:?}-{:?}] contract address: {:?}",
                        chunk_start, chunk_end, contract_address
                    );
                    let events = provider
                        .get_logs(
                            &Filter::new()
                                .from_block(chunk_start)
                                .to_block(chunk_end)
                                .address(contract_address)
                                .event_signature(filter_events),
                        )
                        .await?;

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
}

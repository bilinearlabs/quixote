// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event collector.

use crate::{LogChunk, TxLogChunk, constants::*};
use alloy::{
    eips::BlockNumberOrTag, json_abi::Event, primitives::Address, providers::Provider,
    rpc::types::Filter,
};
use anyhow::Result;
use futures::stream::{self, TryStreamExt};
use std::{error::Error, sync::Arc};
use tokio::time::{Duration, sleep};

#[derive(Clone)]
pub struct EventCollector {
    contract_address: Address,
    event: Option<Event>,
    start_block: u64,
    provider: Arc<dyn Provider + Send + Sync>,
    sync_mode: BlockNumberOrTag,
    poll_interval: u64,
    producer_buffer: TxLogChunk,
}

impl EventCollector {
    pub fn new(
        contract_address: Address,
        event: Option<Event>,
        start_block: u64,
        provider: Arc<dyn Provider + Send + Sync>,
        sync_mode: BlockNumberOrTag,
        producer_buffer: TxLogChunk,
    ) -> Self {
        Self {
            contract_address,
            event,
            start_block,
            provider,
            sync_mode,
            poll_interval: DEFAULT_POLL_INTERVAL,
            producer_buffer,
        }
    }

    pub async fn collect(&self) -> Result<()> {
        let mut processed_to = self.start_block.saturating_sub(1);

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

            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let provider_clone = self.provider.clone();
            stream::iter(
                chunk_starts
                    .into_iter()
                    .map(Ok::<u64, Box<dyn Error + Send + Sync>>),
            )
            .try_for_each_concurrent(MAX_CONCURRENT_RPC_REQUESTS, |chunk_start| {
                let tx = producer_buffer.clone();
                let provider = provider_clone.clone();
                let contract_address = contract_address;
                let event = self.event.clone();
                async move {
                    let chunk_end = std::cmp::min(
                        chunk_start + DEFAULT_BLOCK_RANGE as u64 - 1,
                        finalized_block,
                    );

                    println!(
                        "Fetching events for blocks [{:?}-{:?}] contract address: {:?}",
                        chunk_start, chunk_end, contract_address
                    );

                    // Build the filter for the get_Logs call.
                    let mut filter = Filter::new()
                        .from_block(chunk_start)
                        .to_block(chunk_end)
                        .address(contract_address);

                    // If we are indexing a single event, we can ask for a filtered response,
                    // otherwise we will get all events.
                    if let Some(event) = event {
                        filter = filter.event_signature(event.selector());
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
            .await
            .map_err(|e| anyhow::anyhow!("Error collecting events: {}", e))?;

            // All blocks up to `finalized_block` have been queued for processing.
            processed_to = finalized_block;
        }
    }
}

// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Runner module for the event collector.

use crate::{RpcHost, TxLogChunk, constants::*, event_collector::EventCollector};
use alloy::{
    eips::BlockNumberOrTag,
    json_abi::Event,
    primitives::Address,
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
use tokio::sync::Semaphore;

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
pub struct EventCollectorRunner {
    provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>>,
    contract_address: Address,
    events: Vec<Event>,
    start_block: BlockNumberOrTag,
    producer_buffer: TxLogChunk,
}

impl EventCollectorRunner {
    pub fn new(
        host_list: &[RpcHost],
        contract_address: Address,
        events: Vec<Event>,
        start_block: BlockNumberOrTag,
        producer_buffer: TxLogChunk,
    ) -> Result<Self> {
        let always_retry_policy = AlwaysRetryPolicy::default();

        let retry_policy = RetryBackoffLayer::new_with_policy(
            DEFAULT_BACKOFF_LAYER_MAX_RETRIES,
            DEFAULT_BACKOFF_LAYER_BACKOFF_TIME,
            DEFAULT_BACKOFF_LAYER_CUP_SIZE,
            always_retry_policy,
        );

        let mut provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>> = Vec::new();
        for host in host_list {
            let provider = ProviderBuilder::new().connect_client(
                RpcClient::builder()
                    .layer(retry_policy.clone())
                    .http(host.try_into().map_err(|e| {
                        anyhow::anyhow!("Failed to convert RPC host to URL: {}", e)
                    })?),
            );
            provider_list.push(Arc::new(provider));
        }

        Ok(Self {
            provider_list,
            contract_address,
            events,
            start_block,
            producer_buffer,
        })
    }

    pub async fn run(&self) -> Result<()> {
        println!("Starting the event collector runner");

        if self.provider_list.is_empty() {
            anyhow::bail!("No providers available");
        }

        // At this point, the start_block is always a number, as the upper layer selects the starting block based on
        // the current DB status and the user's input.
        let start_block_num = self.start_block.as_number().unwrap();
        println!("Start block: {start_block_num}");
        println!(
            "Spawning {} collector tasks (one per RPC host)",
            self.provider_list.len()
        );

        // Semaphore sized to the number of RPC hosts - one collector per host
        let semaphore = Arc::new(Semaphore::new(self.provider_list.len()));
        // Collect all task handles
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Spawn one EventCollector per RPC host
        for (host_index, provider) in self.provider_list.iter().enumerate() {
            let provider = provider.clone();
            let semaphore = semaphore.clone();
            let contract_address = self.contract_address;
            let producer_buffer = self.producer_buffer.clone();
            let start_block_num = start_block_num;

            // When we've got multiple events, we are expecting to index the entire ABI,
            // so we pass None and no filtering is applied. Otherwise, filter by the single event.
            let event = if self.events.len() == 1 {
                Some(self.events[0].clone())
            } else {
                None
            };

            let handle = tokio::spawn(async move {
                // Acquire permit (one per RPC host)
                let _permit = semaphore.acquire().await.unwrap();

                println!(
                    "Spawning collector for RPC host {} (starting from block {})",
                    host_index, start_block_num
                );

                let collector = EventCollector::new(
                    contract_address,
                    event,
                    start_block_num,
                    provider,
                    BlockNumberOrTag::Finalized,
                    producer_buffer,
                );

                let collector_handle = tokio::spawn(async move {
                    // Collect events - this runs forever, polling for new blocks
                    let _ = collector.collect().await;
                });

                if let Err(e) = collector_handle.await {
                    eprintln!("Collector task for RPC host {} panicked: {}", host_index, e);
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete (they run forever, so this will only return on error)
        for handle in handles {
            handle.await?;
        }

        println!("All collector tasks completed");
        Ok(())
    }
}

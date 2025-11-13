// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Runner module for the event collector.

use crate::{EventCollector, RpcHost, TxLogChunk};
use alloy::providers::Provider;
use alloy::{
    eips::BlockNumberOrTag,
    primitives::Address,
    providers::ProviderBuilder,
    rpc::client::RpcClient,
    transports::{
        TransportError,
        layers::{RetryBackoffLayer, RetryPolicy},
    },
};
use anyhow::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;
//use dashmap::DashMap;

const DEFAULT_BLOCK_RANGE: u64 = 10000;
const MAX_CONCURRENT_REQUESTS: usize = 1;

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

pub struct EventCollectorRunner {
    provider_list: Vec<Arc<dyn Provider + Send + Sync + 'static>>,
    contract_address: Address,
    events: Vec<String>,
    start_block: BlockNumberOrTag,
    producer_buffer: TxLogChunk,
}

impl EventCollectorRunner {
    pub fn new(
        host_list: &[RpcHost],
        contract_address: Address,
        events: Vec<String>,
        start_block: BlockNumberOrTag,
        producer_buffer: TxLogChunk,
    ) -> Result<Self> {
        let max_retry: u32 = 100;
        let backoff: u64 = 2000;
        let cups: u64 = 100;
        let always_retry_policy = AlwaysRetryPolicy::default();

        let retry_policy =
            RetryBackoffLayer::new_with_policy(max_retry, backoff, cups, always_retry_policy);

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
        println!("Running event collector");

        // Get the latest block number to know when to stop
        let latest_block = if let Some(provider) = self.provider_list.first() {
            println!("Getting latest block number");
            provider.get_block_number().await?
        } else {
            anyhow::bail!("No providers available");
        };

        println!("Start block: {}", self.start_block.as_number().unwrap());
        println!("Latest block number: {}", latest_block);

        // Get the starting block number
        let start_block_num = match self.start_block {
            BlockNumberOrTag::Number(n) => n,
            BlockNumberOrTag::Latest => latest_block,
            _ => anyhow::bail!("Unsupported block number tag"),
        };

        // Semaphore to limit concurrent requests
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
        // Atomic counter to track the current chunk
        let current_chunk = Arc::new(AtomicU64::new(0));
        // Collect all task handles
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        println!("Spawning up to {} tasks", MAX_CONCURRENT_REQUESTS);

        // Spawn up to MAX_CONCURRENT_REQUESTS tasks
        for task_id in 0..MAX_CONCURRENT_REQUESTS {
            // Assign each task a different provider from the list (round-robin based on task_id)
            let provider_index = task_id % self.provider_list.len();
            let provider = self.provider_list[provider_index].clone();

            println!(
                "Provider {}: {} of {}",
                task_id,
                provider_index,
                self.provider_list.len()
            );

            let semaphore = semaphore.clone();
            let current_chunk = current_chunk.clone();
            let contract_address = self.contract_address;
            let events = self.events.clone();
            let producer_buffer = self.producer_buffer.clone();
            let start_block_num = start_block_num;
            let latest_block = latest_block;

            let handle = tokio::spawn(async move {
                loop {
                    // Acquire permit to limit concurrency
                    let _permit = semaphore.acquire().await.unwrap();

                    // Get the next chunk to process
                    let chunk_index = current_chunk.fetch_add(1, Ordering::SeqCst);
                    let chunk_start = start_block_num + (chunk_index * DEFAULT_BLOCK_RANGE);

                    // Check if we've reached the latest block
                    if chunk_start > latest_block {
                        break;
                    }

                    let chunk_end =
                        std::cmp::min(chunk_start + DEFAULT_BLOCK_RANGE - 1, latest_block);

                    println!(
                        "Processing chunk {}: blocks {} to {}",
                        chunk_index, chunk_start, chunk_end
                    );

                    // Create event collector for this chunk
                    let collector = EventCollector::new(
                        contract_address,
                        events.clone(),
                        chunk_start,
                        provider.clone(),
                        BlockNumberOrTag::Finalized,
                        producer_buffer.clone(),
                    );

                    println!(
                        "Task {} processing chunk {}: blocks {} to {}",
                        task_id, chunk_index, chunk_start, chunk_end
                    );

                    // Collect events for this chunk
                    if let Err(e) = collector.collect().await {
                        eprintln!(
                            "Task {} error collecting events for chunk {}: {}",
                            task_id, chunk_index, e
                        );
                    }
                }
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await?;
        }

        println!("All collector tasks completed");
        Ok(())
    }
}

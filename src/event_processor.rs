// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event processor.

use crate::{CancellationToken, RxLogChunk, metrics::MetricsHandle, storage::Storage};
use alloy::rpc::types::Log;
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::select;
use tracing::{debug, error, info};

pub struct EventProcessor {
    chain_id: u64,
    storage: Arc<dyn Storage + Send + Sync>,
    start_block: u64,
    producer_buffer: RxLogChunk,
    cancellation_token: CancellationToken,
    metrics: MetricsHandle,
}

impl EventProcessor {
    pub fn new(
        chain_id: u64,
        storage: Arc<dyn Storage + Send + Sync>,
        start_block: u64,
        producer_buffer: RxLogChunk,
        cancellation_token: CancellationToken,
        metrics: MetricsHandle,
    ) -> Self {
        Self {
            chain_id,
            storage,
            start_block,
            producer_buffer,
            cancellation_token,
            metrics,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut cancellation_receiver = self.cancellation_token.subscribe();
        let mut last_processed = self.start_block.saturating_sub(1);
        let mut buffer: BTreeMap<u64, (u64, Vec<Log>)> = BTreeMap::new();
        let last_processed_shared = Mutex::new(last_processed);

        loop {
            select! {
                _ = cancellation_receiver.recv() => {
                    debug!("Producer::Cancellation requested, shutting down gracefully...");
                    // Sometimes, if no events are detected, the first block gest registered but the last block remains
                    // as 0. This is an invalid state.
                    self.storage.synchronize_events(Some(*last_processed_shared.lock().unwrap()))?;
                    return Ok(());
                }
                events = self.producer_buffer.recv() => {
                    match events {
                        Some(log_chunk) => {
                            buffer.insert(log_chunk.start_block, (log_chunk.end_block, log_chunk.events));

                            // Try to process as many contiguous chunks as possible.  The next
                            // expected chunk must start exactly at `last_processed + 1`.
                            while let Some((end, ev)) = buffer.remove(&(last_processed + 1)) {
                                // If the chunk includes no events, just update that we have processed up to the last
                                // block.
                                if ev.is_empty() {
                                    self.storage.synchronize_events(Some(last_processed))?;
                                    continue;
                                }

                                if let Err(e) = self.storage.add_events(ev.as_slice()) {
                                    error!("Error adding events: {}", e);
                                    // Ensure the database is in a consistent state.
                                    self.storage.synchronize_events(Some(last_processed))?;
                                    return Err(e);
                                } else {
                                    info!("Stored events from blocks [{}-{}]", last_processed + 1, end);
                                }

                                // Update the cursor so that the next expected start is directly
                                // after the `end` we just processed.
                                last_processed = end;
                                *last_processed_shared.lock().unwrap() = last_processed;

                                debug!("Processed events from blocks [{}-{}]", last_processed + 1, end);
                                let contract_address = ev.first().unwrap().address().to_string();
                                self.metrics.record_indexed_block(self.chain_id, &contract_address, last_processed);
                            }
                        }
                        None => {
                            // Channel closed, producer is done
                            info!("Event channel closed, all events processed");
                            // Ensure the database is in a consistent state.
                            self.storage.synchronize_events(Some(*last_processed_shared.lock().unwrap()))?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

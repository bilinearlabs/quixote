// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event processor.

use crate::{CancellationToken, RxLogChunk, Storage};
use alloy::rpc::types::Log;
use anyhow::Result;
use std::{collections::BTreeMap, sync::Arc};
use tokio::select;

pub struct EventProcessor {
    storage: Arc<dyn Storage + Send + Sync>,
    start_block: u64,
    producer_buffer: RxLogChunk,
    cancellation_token: CancellationToken,
}

impl EventProcessor {
    pub fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        start_block: u64,
        producer_buffer: RxLogChunk,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            storage,
            start_block,
            producer_buffer,
            cancellation_token,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        let mut cancellation_receiver = self.cancellation_token.subscribe();
        let mut last_processed = self.start_block.saturating_sub(1);
        let mut buffer: BTreeMap<u64, (u64, Vec<Log>)> = BTreeMap::new();

        loop {
            select! {
                _ = cancellation_receiver.recv() => {
                    println!("Producer::Cancellation requested, shutting down gracefully...");
                    return Ok(());
                }
                events = self.producer_buffer.recv() => {
                    match events {
                        Some(log_chunk) => {
                            buffer.insert(log_chunk.start_block, (log_chunk.end_block, log_chunk.events));

                            // Try to process as many contiguous chunks as possible.  The next
                            // expected chunk must start exactly at `last_processed + 1`.
                            while let Some((end, ev)) = buffer.remove(&(last_processed + 1)) {
                                // TODO: Maybe add the start and end block chunks.
                                if let Err(e) = self.storage.add_events(&ev.as_slice()) {
                                    eprintln!("Error adding events: {}", e);
                                    return Err(e);
                                }
                                // Update the cursor so that the next expected start is directly
                                // after the `end` we just processed.
                                last_processed = end;


                            println!("Processed events from blocks [{}-{}]", last_processed + 1, end);
                            }
                        }
                        None => {
                            // Channel closed, producer is done
                            println!("Event channel closed, all events processed");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

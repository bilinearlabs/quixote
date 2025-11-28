// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the event processor.

use crate::{
    CancellationToken, RxLogChunk,
    storage::{DuckDBStorage, Storage},
};
use alloy::rpc::types::Log;
use anyhow::Result;
use std::{collections::BTreeMap, sync::Arc};
use tokio::select;
use tracing::{debug, error, info};

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
                    debug!("Producer::Cancellation requested, shutting down gracefully...");
                    // Sometimes, if no events are detected, the first block gest registered but the last block remains
                    // as 0. This is an invalid state.
                    // TODO: fix this
                    //self.fix_inconsistent_database_state()?;
                    return Ok(());
                }
                events = self.producer_buffer.recv() => {
                    match events {
                        Some(log_chunk) => {
                            buffer.insert(log_chunk.start_block, (log_chunk.end_block, log_chunk.events));

                            // Try to process as many contiguous chunks as possible.  The next
                            // expected chunk must start exactly at `last_processed + 1`.
                            while let Some((end, ev)) = buffer.remove(&(last_processed + 1)) {
                                if let Err(e) = self.storage.add_events(ev.as_slice()) {
                                    error!("Error adding events: {}", e);
                                    // Ensure the database is in a consistent state.
                                    // TODO: fix this
                                    //self.fix_inconsistent_database_state()?;
                                    return Err(e);
                                } else {
                                    tracing::info!("Stored events from blocks [{}-{}]", last_processed + 1, end);
                                }
                                // Update the cursor so that the next expected start is directly
                                // after the `end` we just processed.
                                last_processed = end;


                            debug!("Processed events from blocks [{}-{}]", last_processed + 1, end);
                            }
                        }
                        None => {
                            // Channel closed, producer is done
                            info!("Event channel closed, all events processed");
                            // Ensure the database is in a consistent state.
                            // TODO: fix this
                            //self.fix_inconsistent_database_state()?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    fn fix_inconsistent_database_state(&self) -> Result<()> {
        // let start_block = self.storage.first_block()?;
        // let last_block = self.storage.last_block()?;
        // if last_block < start_block {
        //     tracing::warn!("Fixing inconsistent database state: last_block < start_block");
        //     // Downcast to DuckDBStorage to access set_first_block method. Risky if we end adding more
        //     // storage implementations.
        //     if let Ok(duckdb_storage) = Arc::downcast::<DuckDBStorage>(
        //         self.storage.clone() as Arc<dyn std::any::Any + Send + Sync>
        //     ) {
        //         duckdb_storage.set_first_block(last_block)?;
        //         Ok(())
        //     } else {
        //         error!("Failed to downcast storage to DuckDBStorage");
        //         Err(anyhow::anyhow!(
        //             "Failed to downcast storage to DuckDBStorage"
        //         ))
        //     }
        // } else {
        //     Ok(())
        // }

        todo!()
    }
}

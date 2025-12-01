// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module for the collector seed.
//!
//! # Description
//!
//! Seeds describe the minimal execution unit for an indexing task. Each seed is associated with a contract address
//! and a set of events. All the events within the same seed must maintain a coherent indexing state in the database.
//! This means the indexer won't be able to resume an indexing task from a previous run if the events are disjoint,
//! i.e. they synchronized up to different blocks if the ABI mode is selected.

use crate::storage::{DuckDBStorage, DuckDBStorageFactory, Storage};
use alloy::{eips::BlockNumberOrTag, json_abi::Event, primitives::Address, rpc::types::Filter};
use anyhow::Result;
use tracing::warn;

/// Object that represents a seed for a collecting job.
///
/// # Description
///
/// This object includes all the information needed to start an indexing job. The indexing job may fetch one or many
/// events, but as they all belong to the same job, they all need to synchronize up to the same block.
#[derive(Debug, Clone)]
pub struct CollectorSeed {
    pub contract_address: Address,
    pub events: Vec<Event>,
    pub start_block: u64,
    pub sync_mode: BlockNumberOrTag,
    pub filter: Option<Filter>,
}

impl CollectorSeed {
    /// Create a new CollectorSeed object.
    ///
    /// # Description
    ///
    /// This function creates a new CollectorSeed object. It checks if the given events are synchronized up to the same
    /// block and returns a new CollectorSeed object.
    ///
    /// TODO: support filters
    pub async fn new(
        contract_address: Address,
        events: Vec<Event>,
        start_block: u64,
        filter: Option<Filter>,
        db_path: &str,
    ) -> Result<Self> {
        let db_conn = DuckDBStorageFactory::new(db_path.to_string()).create()?;

        // Ensure the given set of events are synchronized up to the same block.
        let stored_start_block = CollectorSeed::check_start_block(&db_conn, &events).await?;

        // If the DB is empty, consider the given start_block, if not resume from the last
        // stored block.
        let start_block = if stored_start_block == 0 {
            start_block
        } else {
            stored_start_block
        };

        if filter.is_some() {
            warn!("The usage of filters is not yet supported. Filters will be ignored.");
        }

        Ok(Self {
            contract_address,
            events,
            start_block,
            sync_mode: BlockNumberOrTag::Finalized,
            filter: None,
        })
    }

    /// Ensure all the given events are synchronized up to the same block.
    async fn check_start_block(db_conn: &DuckDBStorage, events: &[Event]) -> Result<u64> {
        // Check that all the events synchronized the same blocks.
        // If a previous run indexed events using -e multiple times, and the current run is
        // using the -a option, the indexing state for such event might be different. Thus
        // we can't resume indexing these events as a block (using the same eth_getLogs call).
        let current_start_block = db_conn.first_block(events.iter().next().unwrap())?;
        for event in events {
            if current_start_block != db_conn.first_block(event)? {
                return Err(anyhow::anyhow!(
                    "The given events are disjoint. This means that you need to run the indexer using -e for each one of the given events."
                ));
            }
        }

        Ok(current_start_block)
    }
}

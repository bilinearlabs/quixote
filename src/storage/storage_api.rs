// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use alloy::{json_abi::Event, rpc::types::Log};
use anyhow::Result;
use std::any::Any;

/// Trait that defines the API between the producer task and the storage.
pub trait Storage: Send + Sync + 'static + Any {
    /// Adds a list of events to the storage.
    fn add_events(&self, events: &[Log]) -> Result<()>;
    /// Lists the event types that are registered in the storage.
    fn list_indexed_events(&self) -> Result<Vec<Event>>;
    /// Includes a list of events in the storage.
    ///
    /// # Description
    ///
    /// This method shall be used once at the beginning of the application to include the events that
    /// are to going be indexed.
    fn include_events(&self, events: &[Event]) -> Result<()>;
    /// Gets the full signature of an event type by its hash.
    ///
    /// # Description
    ///
    /// The full signature is the string that represents the event type in the ABI.
    /// For example, for the event ERC20 Transfer event the full signature is:
    /// "Transfer(address indexed from,address indexed to,uint256 value)".
    ///
    /// This signature is used to build a an event object.
    fn get_event_signature(&self, event_hash: &str) -> Result<String>;
    /// Gets the latest block number that has been indexed.
    fn last_block(&self, event: &Event) -> Result<u64>;
    /// Gets the first block number that has been indexed.
    fn first_block(&self, event: &Event) -> Result<u64>;
}

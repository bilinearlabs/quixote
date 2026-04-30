// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

use crate::{
    EventStatus,
    storage::{ContractDescriptorDb, EventDescriptorDb},
};
use alloy::{json_abi::Event, primitives::B256, rpc::types::Log};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::any::Any;

/// Trait that defines the API between the producer task and the storage.
#[async_trait]
pub trait Storage: Send + Sync + 'static + Any {
    /// Adds a list of events to the storage for a specific chain.
    async fn add_events(&self, chain_id: u64, events: &[Log]) -> Result<()>;

    /// Lists the events that are registered in the storage along their indexing status.
    async fn list_indexed_events(&self) -> Result<Vec<EventDescriptorDb>>;

    /// Get the status of an event in the database for a specific chain.
    async fn event_index_status(&self, chain_id: u64, event: &Event)
    -> Result<Option<EventStatus>>;

    /// Includes a list of events in the storage for a specific chain.
    ///
    /// # Description
    ///
    /// This method shall be used once at the beginning of the application to include the events that
    /// are to going be indexed.
    async fn include_events(&self, chain_id: u64, events: &[Event]) -> Result<()>;

    /// Gets the full signature of an event type by its hash.
    ///
    /// # Description
    ///
    /// The full signature is the string that represents the event type in the ABI.
    /// For example, for the event ERC20 Transfer event the full signature is:
    /// "Transfer(address indexed from,address indexed to,uint256 value)".
    ///
    /// This signature is used to build a an event object.
    async fn get_event_signature(&self, event_hash: &str) -> Result<String>;

    /// Gets the latest block number that has been indexed for a specific chain.
    async fn last_block(&self, chain_id: u64, event: &Event) -> Result<u64>;

    /// Gets the first block number that has been indexed for a specific chain.
    async fn first_block(&self, chain_id: u64, event: &Event) -> Result<u64>;

    /// Sets the first block number for an event on a specific chain.
    async fn set_first_block(&self, chain_id: u64, event: &Event, block_number: u64) -> Result<()>;

    /// Sets the last block number for the specified events on a specific chain.
    ///
    /// # Description
    ///
    /// Updates the `last_block` field in `event_descriptor` for the events identified by
    /// `event_selectors` and `chain_id`. This allows concurrent indexing jobs to update
    /// only the events they are processing without interfering with other jobs.
    ///
    /// # Arguments
    ///
    /// * `chain_id` - The chain ID to filter events.
    /// * `event_selectors` - The list of event selectors (topic0 hashes) to update.
    /// * `last_processed` - The block number to set as `last_block`. If `None`, uses the
    ///   maximum `last_block` among the specified events.
    async fn synchronize_events(
        &self,
        chain_id: u64,
        event_selectors: &[B256],
        last_processed: Option<u64>,
    ) -> Result<()>;

    /// Sends a raw SQL query to the storage and returns a JSON value.
    async fn send_raw_query(&self, query: &str) -> Result<Value>;

    /// Lists the contracts indexed in the storage.
    async fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>>;

    /// Returns the database schema including all tables and their column definitions.
    async fn describe_database(&self) -> Result<Value>;

    /// Queries a specific event table with validated filter parameters.
    ///
    /// Returns rows as JSON objects with all values serialized as strings.
    /// Both backends normalize their native types (BYTEA, NUMERIC, VARINT) to strings
    /// so callers see a consistent representation regardless of the database engine.
    async fn query_event_table(&self, query: &EventQuery) -> Result<Vec<Value>>;
}

/// A single column from a dynamically created event table.
#[derive(Clone)]
pub struct EventColumn {
    /// The column / event-parameter name, taken verbatim from the ABI.
    pub name: String,
    /// Solidity selector type string, e.g. `"address"`, `"uint256"`, `"bytes32"`.
    pub selector_type: String,
}

/// Filter comparison operator.
#[derive(Clone, Debug)]
pub enum FilterOp {
    Eq,
    In,
    Gt,
    Lt,
    Gte,
    Lte,
}

/// Value associated with a filter: scalar for all ops except `In`, list for `In`.
#[derive(Clone, Debug)]
pub enum FilterValue {
    Scalar(String),
    List(Vec<String>),
}

/// A single column predicate in a structured WHERE clause.
#[derive(Clone, Debug)]
pub struct WhereClause {
    /// Snake_case column name as stored in the database (e.g. `block_number`).
    pub column: String,
    pub op: FilterOp,
    pub value: FilterValue,
}

/// Sort direction for ORDER BY.
#[derive(Clone, Debug, Default)]
pub enum OrderDir {
    #[default]
    Asc,
    Desc,
}

/// Validated query parameters for [`Storage::query_event_table`].
pub struct EventQuery {
    /// Fully-qualified table name, e.g. `event_1_transfer_ddf25`.
    /// Must be constructed by trusted code; never built from raw user input.
    pub table_name: String,
    /// Event-parameter columns (excludes the four fixed columns).
    pub columns: Vec<EventColumn>,
    /// Structured WHERE predicates; each clause is combined with AND.
    pub where_clauses: Vec<WhereClause>,
    /// Column to sort by (snake_case, pre-validated against known columns).
    pub order_by: Option<String>,
    pub order_dir: OrderDir,
    /// Maximum rows to return (LIMIT); caller must cap at 10 000.
    pub first: u64,
    /// Rows to skip before returning (OFFSET).
    pub skip: u64,
}

/// Trait for creating storage instances.
///
/// # Description
///
/// This trait abstracts over the creation of storage instances for concurrent access. Needed to allow passing
/// different connections to the indexer and the API server when the underlying DB engine does not support
/// concurrent access.
pub trait StorageFactory: Send + Sync {
    /// Creates or returns a storage instance suitable for concurrent access.
    fn create_storage(&self) -> Result<Box<dyn Storage>>;
}

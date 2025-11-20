// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use crate::storage::{ContractDescriptorDb, EventDb, EventDescriptorDb};
use alloy::{json_abi::Event, primitives::Address};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;

/// Trait that defines the API between the REST API and the internal storage.
pub trait StorageQuery {
    fn list_events(&self) -> Result<Vec<EventDescriptorDb>>;
    fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>>;
    fn get_events(
        &self,
        event: Event,
        contract: Address,
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<EventDb>>;
    fn send_raw_query(&self, query: &str) -> Result<Value>;
}

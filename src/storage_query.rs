// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

use alloy::{json_abi::Event, primitives::Address};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct EventDescriptorDb {
    pub event_hash: String,
    pub event_signature: String,
    pub event_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventDb {
    pub block_number: u64,
    pub transaction_hash: String,
    pub log_index: u64,
    pub contract_address: Address,
    pub topic0: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    #[serde(serialize_with = "serialize_timestamp")]
    pub block_timestamp: u64,
}

fn serialize_timestamp<S>(timestamp: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Serialize as ISO 8601 string
    let dt = DateTime::<Utc>::from_timestamp(*timestamp as i64, 0)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp(0, 0).unwrap());
    serializer.serialize_str(&dt.to_rfc3339())
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractDescriptorDb {
    pub contract_address: String,
    pub contract_name: Option<String>,
}

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

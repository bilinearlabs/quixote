// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module that handles the connection to the PostgreSQL database.

use crate::{
    EventStatus,
    storage::{ContractDescriptorDb, EventDescriptorDb, Storage},
};
use alloy::{json_abi::Event, primitives::B256, rpc::types::Log};
use anyhow::Result;
use serde_json::Value;
use sqlx::{Pool, Postgres};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct PostgreSqlStorage {
    conn: Pool<Postgres>,
    strict_mode: bool,
    event_descriptors: Arc<RwLock<HashMap<String, Event>>>,
}

impl Storage for PostgreSqlStorage {
    fn add_events(&self, _chain_id: u64, _events: &[Log]) -> Result<()> {
        Ok(())
    }

    fn list_indexed_events(&self) -> Result<Vec<EventDescriptorDb>> {
        Ok(vec![])
    }

    fn event_index_status(&self, _chain_id: u64, _event: &Event) -> Result<Option<EventStatus>> {
        Ok(None)
    }

    fn include_events(&self, _chain_id: u64, _events: &[Event]) -> Result<()> {
        Ok(())
    }

    fn get_event_signature(&self, _event_hash: &str) -> Result<String> {
        Ok(String::new())
    }

    fn last_block(&self, _chain_id: u64, _event: &Event) -> Result<u64> {
        Ok(0)
    }

    fn first_block(&self, _chain_id: u64, _event: &Event) -> Result<u64> {
        Ok(0)
    }

    fn set_first_block(&self, _chain_id: u64, _event: &Event, _block_number: u64) -> Result<()> {
        // TODO: Implement PostgreSQL set_first_block
        Ok(())
    }

    fn synchronize_events(
        &self,
        _chain_id: u64,
        _event_selectors: &[B256],
        _last_processed: Option<u64>,
    ) -> Result<()> {
        Ok(())
    }

    fn send_raw_query(&self, _query: &str) -> Result<Value> {
        Ok(Value::Null)
    }

    fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> {
        Ok(vec![])
    }

    fn describe_database(&self) -> Result<Value> {
        // TODO: Implement PostgreSQL schema introspection
        Ok(Value::Array(vec![]))
    }
}

impl PostgreSqlStorage {
    pub fn new(conn: Pool<Postgres>) -> Self {
        Self {
            conn,
            strict_mode: false,
            event_descriptors: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_strict_mode(&mut self, strict_mode: bool) {
        self.strict_mode = strict_mode;
    }
}

impl super::StorageFactory for PostgreSqlStorage {
    fn create_storage(&self) -> Result<Box<dyn super::Storage>> {
        // Cloning is cheap - Pool is Arc internally
        Ok(Box::new(self.clone()))
    }
}

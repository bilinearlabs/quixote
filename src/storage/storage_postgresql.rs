// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module that handles the connection to the PostgreSQL database.

use crate::{
    EventStatus,
    storage::{ContractDescriptorDb, EventDescriptorDb, Storage},
};
use alloy::{
    dyn_abi::{DecodedEvent, DynSolValue, EventExt},
    json_abi::Event,
    primitives::B256,
    rpc::types::Log,
};
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Column, Pool, Postgres, Row, TypeInfo, types::Decimal};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct PostgreSqlStorage {
    conn: Pool<Postgres>,
    strict_mode: bool,
    event_descriptors: Arc<RwLock<HashMap<String, Event>>>,
}

impl Storage for PostgreSqlStorage {
    fn add_events(
        &self,
        chain_id: u64,
        events: &[Log],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let pool = self.conn.clone();
        let events = events.to_vec();
        let event_descriptors = self.event_descriptors.clone();
        let strict_mode = self.strict_mode;

        Box::pin(async move {
            // Quick check to avoid unnecessary operations.
            if events.is_empty() {
                info!("No events for the given block range");
                return Ok(());
            }

            // Start a transaction
            let mut tx = pool.begin().await?;

            // First stage: insert the new blocks into the chain-specific blocks table.
            // Use INSERT ... ON CONFLICT DO NOTHING to ignore duplicates without aborting the transaction.
            let mut last_block_number = 0u64;
            for event in &events {
                if let Some(block_number) = event.block_number
                    && block_number != last_block_number
                {
                    let block_hash = event
                        .block_hash
                        .map(|h| h.as_slice().to_vec())
                        .unwrap_or_default();
                    let block_timestamp = event.block_timestamp.unwrap_or_default();

                    let insert_block = format!(
                        "INSERT INTO blocks_{} (block_number, block_hash, block_timestamp) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        chain_id
                    );
                    sqlx::query(&insert_block)
                        .bind(Decimal::from(block_number))
                        .bind(&block_hash)
                        .bind(Decimal::from(block_timestamp))
                        .execute(&mut *tx)
                        .await?;

                    last_block_number = block_number;
                }
            }

            // Second stage: insert the new events into the event_X table.

            // Group events by table so all the events going to the same table can be later inserted at once.
            let mut events_by_table: HashMap<String, Vec<&Log>> = HashMap::new();

            for log in &events {
                if log.block_number.is_none()
                    || log.topic0().is_none()
                    || log.transaction_hash.is_none()
                {
                    continue;
                }

                let event_hash = log.topic0().unwrap().to_string();
                let short_hash = PostgreSqlStorage::short_hash(&event_hash);
                let cache_key = format!("{}:{}", chain_id, short_hash);

                let event_desc = event_descriptors
                    .read()
                    .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?
                    .get(&cache_key)
                    .cloned();

                match event_desc {
                    Some(event) => {
                        let table_name = format!(
                            "event_{}_{}_{}",
                            chain_id,
                            event.name.as_str().to_ascii_lowercase(),
                            short_hash
                        );
                        events_by_table.entry(table_name).or_default().push(log);
                    }
                    None => {
                        if strict_mode {
                            return Err(anyhow::anyhow!(
                                "Event {event_hash} on chain {chain_id:#x} not found. Include it or disable strict mode."
                            ));
                        } else {
                            warn!("Event {event_hash} on chain {chain_id:#x} not found, skipping.");
                        }
                    }
                }
            }

            // Process each table's events
            for (table_name, table_events) in events_by_table {
                if table_events.is_empty() {
                    continue;
                }

                let short_hash = table_name.split('_').nth(3).unwrap();
                let cache_key = format!("{}:{}", chain_id, short_hash);

                let parsed_event = event_descriptors
                    .read()
                    .map_err(|_| anyhow::anyhow!("Failed to acquire read lock"))?
                    .get(&cache_key)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Event descriptor not found"))?;

                let event_hash = table_events
                    .first()
                    .and_then(|l| l.topic0())
                    .map(|h| h.to_string())
                    .unwrap_or_default();
                let mut max_block_number: u64 = 0;

                for log in table_events {
                    let block_number = log.block_number.unwrap();
                    let tx_hash = log.transaction_hash.unwrap().as_slice().to_vec();
                    let log_index = log.log_index.unwrap() as i32;
                    let contract_address = log.address().as_slice().to_vec();

                    max_block_number = max_block_number.max(block_number);

                    // Decode event parameters - store value and whether it's a uint256
                    let mut param_values: Vec<(String, bool)> = Vec::new();
                    if let Ok(DecodedEvent { indexed, body, .. }) = parsed_event
                        .decode_log_parts(log.topics().to_vec(), log.data().data.as_ref())
                    {
                        // Process indexed parameters
                        for (i, item) in indexed.iter().enumerate() {
                            let is_uint256 = parsed_event
                                .inputs
                                .get(i)
                                .map(|p| p.selector_type() == "uint256")
                                .unwrap_or(false);
                            param_values.push((
                                PostgreSqlStorage::dyn_sol_value_to_string(item),
                                is_uint256,
                            ));
                        }
                        // Process body parameters (non-indexed)
                        let indexed_count = indexed.len();
                        for (i, item) in body.iter().enumerate() {
                            let is_uint256 = parsed_event
                                .inputs
                                .get(indexed_count + i)
                                .map(|p| p.selector_type() == "uint256")
                                .unwrap_or(false);
                            param_values.push((
                                PostgreSqlStorage::dyn_sol_value_to_string(item),
                                is_uint256,
                            ));
                        }
                    }

                    // Build column names for this event
                    let param_names: Vec<String> = parsed_event
                        .inputs
                        .iter()
                        .map(|p| format!("\"{}\"", p.name))
                        .collect();
                    let all_columns = format!(
                        "block_number, transaction_hash, log_index, contract_address{}",
                        if param_names.is_empty() {
                            String::new()
                        } else {
                            format!(", {}", param_names.join(", "))
                        }
                    );

                    // Build placeholders
                    let placeholder_count = 4 + param_values.len();
                    let placeholders: Vec<String> =
                        (1..=placeholder_count).map(|i| format!("${}", i)).collect();

                    let insert_sql = format!(
                        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
                        table_name,
                        all_columns,
                        placeholders.join(", ")
                    );

                    let mut query = sqlx::query(&insert_sql)
                        .bind(Decimal::from(block_number))
                        .bind(&tx_hash)
                        .bind(log_index)
                        .bind(&contract_address);

                    for (val, is_uint256) in &param_values {
                        if *is_uint256 {
                            // Parse as Decimal for NUMERIC columns
                            let decimal_val = val.parse::<Decimal>().unwrap_or_default();
                            query = query.bind(decimal_val);
                        } else {
                            query = query.bind(val);
                        }
                    }

                    query.execute(&mut *tx).await?;
                }

                // Update last_block for this event
                let event_hash_bytes =
                    hex::decode(event_hash.trim_start_matches("0x")).unwrap_or_default();
                sqlx::query!(
                    "UPDATE event_descriptor SET last_block = $1 WHERE chain_id = $2 AND event_hash = $3",
                    Decimal::from(max_block_number),
                    Decimal::from(chain_id),
                    event_hash_bytes,
                )
                .execute(&mut *tx)
                .await?;
            }

            tx.commit().await?;
        Ok(())
        })
    }

    fn list_indexed_events(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<EventDescriptorDb>>> + Send + '_>> {
        let pool = self.conn.clone();

        Box::pin(async move {
            // Query all event descriptors
            let rows = sqlx::query!(
                "SELECT chain_id, event_hash, event_signature, event_name, first_block, last_block FROM event_descriptor"
            )
            .fetch_all(&pool)
            .await?;

            let mut events: Vec<EventDescriptorDb> = rows
                .into_iter()
                .map(|row| EventDescriptorDb {
                    chain_id: Some(u64::try_from(row.chain_id).unwrap_or(0)),
                    event_hash: Some(format!("0x{}", hex::encode(&row.event_hash))),
                    event_signature: Some(
                        String::from_utf8_lossy(&row.event_signature).to_string(),
                    ),
                    event_name: Some(row.event_name.clone()),
                    first_block: row.first_block.map(|d| u64::try_from(d).unwrap_or(0)),
                    last_block: row.last_block.map(|d| u64::try_from(d).unwrap_or(0)),
                    event_count: None,
                })
                .collect();

            // Populate event counts for each event
            for event in events.iter_mut() {
                if let (Some(chain_id), Some(event_hash), Some(event_name)) = (
                    event.chain_id,
                    event.event_hash.as_ref(),
                    event.event_name.as_ref(),
                ) {
                    let short_hash = PostgreSqlStorage::short_hash(event_hash);
                    let table_name = format!(
                        "event_{}_{}_{}",
                        chain_id,
                        event_name.to_ascii_lowercase(),
                        short_hash
                    );

                    let count_query = format!("SELECT COUNT(*) FROM {}", table_name);
                    let count: i64 = sqlx::query_scalar(&count_query)
                        .fetch_one(&pool)
                        .await
                        .unwrap_or(0);

                    event.event_count = Some(count as usize);
                }
            }

            Ok(events)
        })
    }

    fn event_index_status(
        &self,
        chain_id: u64,
        event: &Event,
    ) -> Pin<Box<dyn Future<Output = Result<Option<EventStatus>>> + Send + '_>> {
        let pool = self.conn.clone();
        let event_hash = event.selector().to_string();
        let event_hash_bytes = event.selector().as_slice().to_owned();

        Box::pin(async move {
            // Query event_descriptor for this event
            let row = sqlx::query!(
                "SELECT event_name, first_block, last_block FROM event_descriptor WHERE chain_id = $1 AND event_hash = $2",
                Decimal::from(chain_id),
                event_hash_bytes,
            )
            .fetch_optional(&pool)
            .await?;

            let Some(record) = row else {
                return Ok(None);
            };

            let first_block_u64 = record
                .first_block
                .map(|d| u64::try_from(d).unwrap_or(0))
                .unwrap_or(0);
            let last_block_u64 = record
                .last_block
                .map(|d| u64::try_from(d).unwrap_or(0))
                .unwrap_or(0);

            let mut event_status = EventStatus {
                hash: event_hash.clone(),
                first_block: first_block_u64,
                last_block: last_block_u64,
                event_count: 0,
            };

            // If we have processed blocks, count the events
            if last_block_u64 != 0 {
                let short_hash = PostgreSqlStorage::short_hash(&event_hash);
                let table_name = format!(
                    "event_{}_{}_{}",
                    chain_id,
                    record.event_name.to_ascii_lowercase(),
                    short_hash
                );

                let count_query = format!("SELECT COUNT(*) FROM {}", table_name);
                let event_count: i64 = sqlx::query_scalar(&count_query)
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(0);

                event_status.event_count = event_count as usize;
            }

            Ok(Some(event_status))
        })
    }

    fn include_events(
        &self,
        chain_id: u64,
        events: &[Event],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let pool = self.conn.clone();
        // Clone events to own them in the async block
        let events: Vec<Event> = events.to_vec();

        Box::pin(async move {
            // Create the blocks table for this chain
            let create_blocks_table = format!(
                r#"
                CREATE TABLE IF NOT EXISTS blocks_{chain_id}(
                    block_number NUMERIC(20,0) NOT NULL,
                    block_hash BYTEA NOT NULL,
                    block_timestamp NUMERIC(20,0) NOT NULL,
                    PRIMARY KEY (block_number)
                )
                "#
            );
            sqlx::query(&create_blocks_table)
                .execute(&pool)
                .await
                .context("Failed to create blocks table")?;

            // Create tables and register each event
            for event in &events {
                // Table name format: event_<chain_id>_<name>_<short_hash>
                let event_hash = event.selector().to_string();
                let short_hash = PostgreSqlStorage::short_hash(&event_hash);
                let event_name = event.name.as_str().to_ascii_lowercase();

                debug!(
                    "Creating event schema for chain {:#x}: {}",
                    chain_id,
                    event.full_signature()
                );

                // Build the table definition
                let mut create_table = format!(
                    "CREATE TABLE IF NOT EXISTS event_{chain_id}_{event_name}_{short_hash}(
                        block_number NUMERIC(20,0) NOT NULL,
                        transaction_hash BYTEA NOT NULL,
                        log_index INT NOT NULL,
                        contract_address BYTEA NOT NULL,"
                );

                for param in &event.inputs {
                    let db_type = if param.selector_type() == "uint256" {
                        "NUMERIC(78,0)"
                    } else {
                        "TEXT"
                    };
                    create_table.push_str(&format!("\"{}\" {db_type},", param.name));
                }

                create_table.push_str("PRIMARY KEY (block_number, transaction_hash, log_index))");

                sqlx::query(&create_table).execute(&pool).await?;

                // Create an entry for this event in the event_descriptor table
                let event_hash_bytes = event.selector().as_slice().to_vec();
                let event_signature_bytes = event.full_signature().as_bytes().to_vec();

                sqlx::query(
                    "INSERT INTO event_descriptor (chain_id, event_hash, event_signature, event_name, first_block, last_block) \
                     VALUES ($1, $2, $3, $4, 0, 0) \
                     ON CONFLICT (chain_id, event_hash) DO NOTHING"
                )
                    .bind(Decimal::from(chain_id))
                    .bind(&event_hash_bytes)
                    .bind(&event_signature_bytes)
                    .bind(&event.name)
                    .execute(&pool)
                    .await
                    .context("Failed to insert event descriptor")?;
            }

            let mut event_descriptors = self.event_descriptors.write().map_err(|_| {
                anyhow::anyhow!("Failed to acquire write lock for the event descriptor")
            })?;

            // Populate the local cache of the indexed events.
            // Key is chain_id:short_hash to support same event on different chains.
            for event in events {
                let selector_str = event.selector().to_string();
                let short_hash = Self::short_hash(&selector_str);
                let key = format!("{}:{}", chain_id, short_hash);
                event_descriptors.insert(key, event.clone());
            }

        Ok(())
        })
    }

    fn get_event_signature(
        &self,
        event_hash: &str,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + '_>> {
        let pool = self.conn.clone();
        // Convert hex string to bytes (strip 0x prefix if present)
        let hash_str = event_hash.strip_prefix("0x").unwrap_or(event_hash);
        let event_hash_bytes = hex::decode(hash_str).unwrap_or_default();

        Box::pin(async move {
            let signature: Vec<u8> = sqlx::query_scalar!(
                "SELECT event_signature FROM event_descriptor WHERE event_hash = $1",
                event_hash_bytes,
            )
            .fetch_one(&pool)
            .await?;

            Ok(String::from_utf8_lossy(&signature).to_string())
        })
    }

    fn last_block(
        &self,
        chain_id: u64,
        event: &Event,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + '_>> {
        let pool = self.conn.clone();
        let event_hash = event.selector().as_slice().to_owned();

        Box::pin(async move {
            let last_block: Option<Decimal> = sqlx::query_scalar!(
                "SELECT last_block FROM event_descriptor WHERE chain_id = $1 AND event_hash = $2",
                Decimal::from(chain_id),
                event_hash,
            )
            .fetch_one(&pool)
            .await?;

            Ok(last_block
                .map(|d| u64::try_from(d).unwrap_or(0))
                .unwrap_or(0))
        })
    }

    fn first_block(
        &self,
        chain_id: u64,
        event: &Event,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + '_>> {
        let pool = self.conn.clone();
        let event_hash = event.selector().as_slice().to_owned();

        Box::pin(async move {
            let first_block: Option<Decimal> = sqlx::query_scalar!(
                "SELECT first_block FROM event_descriptor WHERE chain_id = $1 AND event_hash = $2",
                Decimal::from(chain_id),
                event_hash,
            )
            .fetch_one(&pool)
            .await?;

            Ok(first_block
                .map(|d| u64::try_from(d).unwrap_or(0))
                .unwrap_or(0))
        })
    }

    fn set_first_block(
        &self,
        chain_id: u64,
        event: &Event,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let pool = self.conn.clone();
        let event_hash = event.selector().as_slice().to_owned();

        Box::pin(async move {
            sqlx::query!(
                "UPDATE event_descriptor SET first_block = $1 WHERE chain_id = $2 AND event_hash = $3",
                Decimal::from(block_number), Decimal::from(chain_id), event_hash,
            )
            .execute(&pool)
            .await?;
        Ok(())
        })
    }

    fn synchronize_events(
        &self,
        chain_id: u64,
        event_selectors: &[B256],
        last_processed: Option<u64>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let pool = self.conn.clone();
        // Convert selectors to owned byte vectors for the async block
        let selectors: Vec<Vec<u8>> = event_selectors
            .iter()
            .map(|s| s.as_slice().to_vec())
            .collect();

        Box::pin(async move {
            if selectors.is_empty() {
                debug!(
                    "No event selectors provided for synchronize_events on chain {:#x}, skipping",
                    chain_id
                );
                return Ok(());
            }

            if let Some(last_block) = last_processed {
                // Update all matching events to the specified last_block
                sqlx::query!(
                    "UPDATE event_descriptor SET last_block = $1 WHERE chain_id = $2 AND event_hash = ANY($3)",
                    Decimal::from(last_block),
                    Decimal::from(chain_id),
                    &selectors as &[Vec<u8>],
                )
                .execute(&pool)
                .await?;
            } else {
                // Set last_block to the MAX among the specified events
                sqlx::query!(
                    "UPDATE event_descriptor SET last_block = (
                        SELECT MAX(last_block) FROM event_descriptor WHERE chain_id = $1 AND event_hash = ANY($2)
                    ) WHERE chain_id = $1 AND event_hash = ANY($2)",
                    Decimal::from(chain_id),
                    &selectors as &[Vec<u8>],
                )
                .execute(&pool)
                .await?;
            }

            debug!(
                "Events synchronized up to block {}",
                last_processed.unwrap_or_default()
            );

        Ok(())
        })
    }

    fn send_raw_query(
        &self,
        query: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + '_>> {
        let pool = self.conn.clone();
        let query = query.to_string();

        Box::pin(async move {
            // Only allow SELECT statements for safety
            if !query.trim_start().to_uppercase().starts_with("SELECT") {
                return Ok(serde_json::json!({ "error": "Query must be a SELECT statement" }));
            }

            let rows = sqlx::query(&query).fetch_all(&pool).await?;

            let mut results = Vec::new();

            for row in rows {
                let mut row_obj = serde_json::Map::new();

                for (i, column) in row.columns().iter().enumerate() {
                    let col_name = column.name().to_string();
                    let type_name = column.type_info().name();

                    // Convert based on PostgreSQL type
                    let value: Value = match type_name {
                        "INT2" | "INT4" => {
                            let v: Option<i32> = row.try_get(i).ok();
                            v.map(|n| Value::Number(n.into())).unwrap_or(Value::Null)
                        }
                        "INT8" => {
                            let v: Option<i64> = row.try_get(i).ok();
                            v.map(|n| Value::Number(n.into())).unwrap_or(Value::Null)
                        }
                        "FLOAT4" | "FLOAT8" => {
                            let v: Option<f64> = row.try_get(i).ok();
                            v.and_then(serde_json::Number::from_f64)
                                .map(Value::Number)
                                .unwrap_or(Value::Null)
                        }
                        "NUMERIC" => {
                            let v: Option<Decimal> = row.try_get(i).ok();
                            v.map(|d| Value::String(d.to_string()))
                                .unwrap_or(Value::Null)
                        }
                        "BOOL" => {
                            let v: Option<bool> = row.try_get(i).ok();
                            v.map(Value::Bool).unwrap_or(Value::Null)
                        }
                        "BYTEA" => {
                            let v: Option<Vec<u8>> = row.try_get(i).ok();
                            v.map(|b| Value::String(format!("0x{}", hex::encode(b))))
                                .unwrap_or(Value::Null)
    }
                        _ => {
                            // Default: try as string
                            let v: Option<String> = row.try_get(i).ok();
                            v.map(Value::String).unwrap_or(Value::Null)
                        }
                    };

                    row_obj.insert(col_name, value);
                }

                results.push(Value::Object(row_obj));
            }

            Ok(Value::Array(results))
        })
    }

    fn list_contracts(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ContractDescriptorDb>>> + Send + '_>> {
        let pool = self.conn.clone();

        Box::pin(async move {
            // Get all event descriptors to find their tables
            let descriptors =
                sqlx::query!("SELECT chain_id, event_hash, event_name FROM event_descriptor")
                    .fetch_all(&pool)
                    .await?;

            let mut contracts = HashSet::new();

            // For each event table, get distinct contract addresses
            for row in descriptors {
                let chain_id = u64::try_from(row.chain_id).unwrap_or(0);
                let event_hash = format!("0x{}", hex::encode(&row.event_hash));
                let short_hash = PostgreSqlStorage::short_hash(&event_hash);
                let table_name = format!(
                    "event_{}_{}_{}",
                    chain_id,
                    row.event_name.to_ascii_lowercase(),
                    short_hash
                );

                let query = format!("SELECT DISTINCT contract_address FROM {}", table_name);
                let addresses: Vec<Vec<u8>> = sqlx::query_scalar(&query)
                    .fetch_all(&pool)
                    .await
                    .unwrap_or_default();

                contracts.extend(
                    addresses
                        .into_iter()
                        .map(|bytes| format!("0x{}", hex::encode(bytes))),
                );
            }

            Ok(contracts
                .into_iter()
                .map(|address| ContractDescriptorDb {
                    contract_address: address,
                    contract_name: None,
                })
                .collect())
        })
    }

    fn describe_database(&self) -> Pin<Box<dyn Future<Output = Result<Value>> + Send + '_>> {
        let pool = self.conn.clone();

        Box::pin(async move {
            // Get all tables in the public schema, excluding quixote_info
            let tables: Vec<String> = sqlx::query_scalar(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name != 'quixote_info'"
            )
            .fetch_all(&pool)
            .await?;

            let mut result = Vec::new();

            // For each table, get its column definitions
            for table_name in tables {
                let columns = sqlx::query!(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
                    table_name
                )
                .fetch_all(&pool)
                .await?;

                let mut columns_map = serde_json::Map::new();
                for col in columns {
                    if let (Some(name), Some(dtype)) = (col.column_name, col.data_type) {
                        columns_map.insert(name, Value::String(dtype));
                    }
                }

                let mut table_obj = serde_json::Map::new();
                table_obj.insert(table_name, Value::Object(columns_map));
                result.push(Value::Object(table_obj));
            }

            Ok(Value::Array(result))
        })
    }
}

impl PostgreSqlStorage {
    /// Length of the short hash used in table names (5 hex characters).
    const SHORT_HASH_LEN: usize = 5;

    /// Returns a shortened version of an event hash for use in table names.
    ///
    /// Takes the first 5 hex characters after the "0x" prefix.
    /// Example: "0xddf252ad..." -> "ddf25"
    #[inline]
    fn short_hash(full_hash: &str) -> &str {
        let start = if full_hash.starts_with("0x") { 2 } else { 0 };
        &full_hash[start..start + Self::SHORT_HASH_LEN]
    }

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

    /// Converts a `DynSolValue` to a String representation for storage.
    fn dyn_sol_value_to_string(value: &DynSolValue) -> String {
        match value {
            DynSolValue::Address(a) => a.to_string().to_lowercase(),
            DynSolValue::Bool(b) => b.to_string(),
            DynSolValue::Int(i, _) => i.to_string(),
            DynSolValue::Uint(u, _) => u.to_string(),
            DynSolValue::String(s) => s.clone(),
            DynSolValue::FixedBytes(bytes, size) => {
                format!("0x{}", hex::encode(&bytes[..(*size).min(32)]))
            }
            DynSolValue::Bytes(bytes) => {
                format!("0x{}", hex::encode(bytes))
            }
            DynSolValue::Function(f) => {
                format!("0x{}", hex::encode(f.as_slice()))
            }
            DynSolValue::Array(values) | DynSolValue::FixedArray(values) => {
                let elements: Vec<String> =
                    values.iter().map(Self::dyn_sol_value_to_string).collect();
                format!("[{}]", elements.join(","))
            }
            DynSolValue::Tuple(values) => {
                let elements: Vec<String> =
                    values.iter().map(Self::dyn_sol_value_to_string).collect();
                format!("({})", elements.join(","))
            }
        }
    }
}

impl super::StorageFactory for PostgreSqlStorage {
    fn create_storage(&self) -> Result<Box<dyn super::Storage>> {
        // Cloning is cheap - Pool is Arc internally
        Ok(Box::new(self.clone()))
    }
}

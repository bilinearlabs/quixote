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
            // Collect unique blocks into vectors for bulk insert using UNNEST.
            let mut seen_blocks = HashSet::new();
            let mut block_numbers: Vec<Decimal> = Vec::new();
            let mut block_hashes: Vec<Vec<u8>> = Vec::new();
            let mut block_timestamps: Vec<Decimal> = Vec::new();

            for event in &events {
                if let Some(block_number) = event.block_number
                    && seen_blocks.insert(block_number)
                {
                    block_numbers.push(Decimal::from(block_number));
                    block_hashes.push(
                        event
                            .block_hash
                            .map(|h| h.as_slice().to_vec())
                            .unwrap_or_default(),
                    );
                    block_timestamps.push(Decimal::from(event.block_timestamp.unwrap_or_default()));
                }
            }

            // Bulk insert all blocks at once using UNNEST
            if !block_numbers.is_empty() {
                let insert_blocks = format!(
                    "INSERT INTO blocks_{} (block_number, block_hash, block_timestamp) \
                     SELECT * FROM UNNEST($1::numeric[], $2::bytea[], $3::numeric[]) \
                     ON CONFLICT DO NOTHING",
                    chain_id
                );
                sqlx::query(&insert_blocks)
                    .bind(&block_numbers)
                    .bind(&block_hashes)
                    .bind(&block_timestamps)
                    .execute(&mut *tx)
                    .await?;
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

            // Process each table's events using bulk insert with UNNEST
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

                // Collect all data into vectors for bulk insert
                let mut block_numbers: Vec<Decimal> = Vec::with_capacity(table_events.len());
                let mut tx_hashes: Vec<Vec<u8>> = Vec::with_capacity(table_events.len());
                let mut log_indices: Vec<i32> = Vec::with_capacity(table_events.len());
                let mut contract_addresses: Vec<Vec<u8>> = Vec::with_capacity(table_events.len());

                // Determine param types: 0 = TEXT, 1 = NUMERIC (uint256), 2 = BYTEA (address)
                let param_types: Vec<u8> = parsed_event
                    .inputs
                    .iter()
                    .map(|p| match p.selector_type().as_ref() {
                        "uint256" => 1,
                        "address" => 2,
                        _ => 0,
                    })
                    .collect();

                // Create vectors for each parameter column type
                let mut numeric_params: Vec<Vec<Decimal>> =
                    vec![Vec::with_capacity(table_events.len()); param_types.len()];
                let mut text_params: Vec<Vec<String>> =
                    vec![Vec::with_capacity(table_events.len()); param_types.len()];
                let mut bytea_params: Vec<Vec<Vec<u8>>> =
                    vec![Vec::with_capacity(table_events.len()); param_types.len()];

                let mut max_block_number: u64 = 0;

                for log in &table_events {
                    let block_number = log.block_number.unwrap();
                    max_block_number = max_block_number.max(block_number);

                    // Decode event parameters first - only add to vectors if decoding succeeds
                    // to ensure all arrays have matching lengths for UNNEST
                    if let Ok(DecodedEvent { indexed, body, .. }) = parsed_event
                        .decode_log_parts(log.topics().to_vec(), log.data().data.as_ref())
                    {
                        // Add fixed columns
                        block_numbers.push(Decimal::from(block_number));
                        tx_hashes.push(log.transaction_hash.unwrap().as_slice().to_vec());
                        log_indices.push(log.log_index.unwrap() as i32);
                        contract_addresses.push(log.address().as_slice().to_vec());

                        // Add parameter columns
                        let all_values: Vec<&DynSolValue> =
                            indexed.iter().chain(body.iter()).collect();

                        for (i, (param_type, value)) in
                            param_types.iter().zip(all_values.iter()).enumerate()
                        {
                            match param_type {
                                1 => {
                                    // NUMERIC (uint256)
                                    let str_val = PostgreSqlStorage::dyn_sol_value_to_string(value);
                                    numeric_params[i]
                                        .push(str_val.parse::<Decimal>().unwrap_or_default());
                                    text_params[i].push(String::new());
                                    bytea_params[i].push(Vec::new());
                                }
                                2 => {
                                    // BYTEA (address)
                                    let bytes = PostgreSqlStorage::dyn_sol_value_to_bytes(value);
                                    bytea_params[i].push(bytes);
                                    numeric_params[i].push(Decimal::default());
                                    text_params[i].push(String::new());
                                }
                                _ => {
                                    // TEXT
                                    let str_val = PostgreSqlStorage::dyn_sol_value_to_string(value);
                                    text_params[i].push(str_val);
                                    numeric_params[i].push(Decimal::default());
                                    bytea_params[i].push(Vec::new());
                                }
                            }
                        }
                    }
                }

                // Build column names
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

                // Build UNNEST types for each column
                let mut unnest_parts = vec![
                    "$1::numeric[]".to_string(),
                    "$2::bytea[]".to_string(),
                    "$3::int[]".to_string(),
                    "$4::bytea[]".to_string(),
                ];
                for (i, param_type) in param_types.iter().enumerate() {
                    let param_idx = 5 + i;
                    match param_type {
                        1 => unnest_parts.push(format!("${}::numeric[]", param_idx)),
                        2 => unnest_parts.push(format!("${}::bytea[]", param_idx)),
                        _ => unnest_parts.push(format!("${}::text[]", param_idx)),
                    }
                }

                // Only execute bulk insert if we have events to insert
                if !block_numbers.is_empty() {
                    let insert_sql = format!(
                        "INSERT INTO {} ({}) SELECT * FROM UNNEST({}) ON CONFLICT DO NOTHING",
                        table_name,
                        all_columns,
                        unnest_parts.join(", ")
                    );

                    // Build and execute the query with all bindings
                    let mut query = sqlx::query(&insert_sql)
                        .bind(&block_numbers)
                        .bind(&tx_hashes)
                        .bind(&log_indices)
                        .bind(&contract_addresses);

                    for (i, param_type) in param_types.iter().enumerate() {
                        match param_type {
                            1 => query = query.bind(&numeric_params[i]),
                            2 => query = query.bind(&bytea_params[i]),
                            _ => query = query.bind(&text_params[i]),
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
                    let db_type = match param.selector_type().as_ref() {
                        "uint256" => "NUMERIC(78,0)",
                        "address" => "BYTEA",
                        _ => "TEXT",
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

    /// Converts a `DynSolValue` to bytes for BYTEA storage (addresses).
    fn dyn_sol_value_to_bytes(value: &DynSolValue) -> Vec<u8> {
        match value {
            DynSolValue::Address(a) => a.as_slice().to_vec(),
            // For non-address types, return empty (should not happen if types are correct)
            _ => Vec::new(),
        }
    }
}

impl super::StorageFactory for PostgreSqlStorage {
    fn create_storage(&self) -> Result<Box<dyn super::Storage>> {
        // Cloning is cheap - Pool is Arc internally
        Ok(Box::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use fake::{Fake, Faker};

    const TEST_CHAIN_ID_MAINNET: u64 = 1;
    const TEST_CHAIN_ID_BASE: u64 = 8453;

    /// Helper function to get event count from DB
    async fn event_count_from_db(
        pool: &Pool<Postgres>,
        chain_id: u64,
        event: &Event,
    ) -> Result<usize> {
        let selector = event.selector().to_string();
        let short_hash = &selector[2..7]; // first 5 chars after "0x"
        let table_name = format!(
            "event_{}_{}_{}",
            chain_id,
            event.name.to_ascii_lowercase(),
            short_hash
        );
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table_name))
            .fetch_one(pool)
            .await?;
        Ok(count as usize)
    }

    /// Helper function to get first block from DB for a given event.
    async fn first_block_from_db(
        pool: &Pool<Postgres>,
        chain_id: u64,
        event: &Event,
    ) -> Result<u64> {
        let rec = sqlx::query!(
            "SELECT first_block FROM event_descriptor WHERE event_hash = $1 AND chain_id = $2",
            event.selector().as_slice().to_vec(),
            chain_id as i64,
        )
        .fetch_one(pool)
        .await?;

        // first_block can be Option<Decimal>
        let first_block = rec.first_block;
        match first_block {
            Some(decimal) => Ok(u64::try_from(decimal).unwrap_or(0)),
            None => anyhow::bail!(
                "First block not found for event {}",
                event.selector().to_string()
            ),
        }
    }

    /// Test case for adding events to the database.
    #[sqlx::test(migrations = "./migrations")]
    async fn add_events(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());
        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;

        // Insert logs containing 2 different event types. Repeat for 2 different chains.
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_approval()
            .with_erc721_transfer()
            .build();

        storage
            .include_events(
                TEST_CHAIN_ID_MAINNET,
                &[approval_event(), erc721_transfer_event()],
            )
            .await?;
        storage
            .include_events(
                TEST_CHAIN_ID_BASE,
                &[approval_event(), erc721_transfer_event()],
            )
            .await?;

        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;
        storage.add_events(TEST_CHAIN_ID_BASE, &logs).await?;

        let inserted_logs_mainnet =
            event_count_from_db(&pool, TEST_CHAIN_ID_MAINNET, &approval_event()).await?
                + event_count_from_db(&pool, TEST_CHAIN_ID_MAINNET, &erc721_transfer_event())
                    .await?;

        let inserted_logs_base = event_count_from_db(&pool, TEST_CHAIN_ID_BASE, &approval_event())
            .await?
            + event_count_from_db(&pool, TEST_CHAIN_ID_BASE, &erc721_transfer_event()).await?;

        assert_eq!(inserted_logs_mainnet, num_logs);
        assert_eq!(inserted_logs_base, num_logs);

        // Test an empty insertion
        assert!(
            storage
                .include_events(TEST_CHAIN_ID_MAINNET, &[])
                .await
                .is_ok()
        );

        Ok(())
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn include_events_idempotent(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());
        let events = vec![transfer_event()];

        // Include the same events twice
        storage.include_events(1, &events).await?;
        storage.include_events(1, &events).await?;

        // Should still have only 1 descriptor (ON CONFLICT DO NOTHING)
        let descriptor_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM event_descriptor WHERE chain_id = 1")
                .fetch_one(&pool)
                .await?;
        assert_eq!(
            descriptor_count, 1,
            "should have 1 event descriptor (idempotent)"
        );

        Ok(())
    }

    /// Test case for setting the first block when starting an indexing task.
    #[sqlx::test(migrations = "./migrations")]
    async fn first_block(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;

        // Insert logs containing 2 different event types. Repeat for 2 different chains.
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_approval()
            .with_erc721_transfer()
            .build();

        storage
            .include_events(
                TEST_CHAIN_ID_MAINNET,
                &[approval_event(), erc721_transfer_event()],
            )
            .await?;

        // Set the starting block for the indexing task. In both cases, the first block is the first block
        // that was retrieved, regardless of whether it contained a particular event.
        storage
            .set_first_block(
                TEST_CHAIN_ID_MAINNET,
                &approval_event(),
                logs[0].block_number.unwrap(),
            )
            .await?;
        storage
            .set_first_block(
                TEST_CHAIN_ID_MAINNET,
                &erc721_transfer_event(),
                logs[0].block_number.unwrap(),
            )
            .await?;

        // Now verify that the initial write of the first block is sound, using the getter
        let saved_first_block =
            first_block_from_db(&pool, TEST_CHAIN_ID_MAINNET, &approval_event()).await?;
        let saved_first_block_erc721_transfer =
            first_block_from_db(&pool, TEST_CHAIN_ID_MAINNET, &erc721_transfer_event()).await?;
        assert_eq!(
            saved_first_block,
            storage
                .first_block(TEST_CHAIN_ID_MAINNET, &approval_event())
                .await?
        );
        assert_eq!(
            saved_first_block_erc721_transfer,
            storage
                .first_block(TEST_CHAIN_ID_MAINNET, &erc721_transfer_event())
                .await?
        );

        assert_eq!(
            saved_first_block,
            logs[0].block_number.unwrap(),
            "first block should be the lowest block number in the logs"
        );
        assert_eq!(
            saved_first_block_erc721_transfer,
            logs[0].block_number.unwrap(),
            "first block should be the lowest block number in the logs"
        );

        Ok(())
    }

    /// Check that the latest block is properly updated when adding events.
    #[sqlx::test(migrations = "./migrations")]
    async fn latest_block(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Need at least 2 logs to ensure both event types are represented (logs cycle through event kinds)
        let num_logs = (Faker.fake::<u8>() % 50 + 2) as usize;

        // Insert logs containing 2 different event types. Repeat for 2 different chains.
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_approval()
            .with_erc721_transfer()
            .build();

        storage
            .include_events(
                TEST_CHAIN_ID_MAINNET,
                &[approval_event(), erc721_transfer_event()],
            )
            .await?;

        // Set the starting block for the indexing task. In both cases, the first block is the first block
        // that was retrieved, regardless of whether it contained a particular event.
        storage
            .set_first_block(
                TEST_CHAIN_ID_MAINNET,
                &approval_event(),
                logs[0].block_number.unwrap(),
            )
            .await?;
        storage
            .set_first_block(
                TEST_CHAIN_ID_MAINNET,
                &erc721_transfer_event(),
                logs[0].block_number.unwrap(),
            )
            .await?;

        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // As we didn't call the synchronize_events method, the latest block should be the highest block that
        // contained the particular event that was inserted.
        let highest_block_approval = logs
            .iter()
            .filter(|log| log.topics()[0] == approval_event().selector())
            .max_by_key(|log| log.block_number.unwrap())
            .unwrap()
            .block_number
            .unwrap();
        let highest_block_erc721_transfer = logs
            .iter()
            .filter(|log| log.topics()[0] == erc721_transfer_event().selector())
            .max_by_key(|log| log.block_number.unwrap())
            .unwrap()
            .block_number
            .unwrap();

        assert_eq!(
            storage
                .last_block(TEST_CHAIN_ID_MAINNET, &approval_event())
                .await?,
            highest_block_approval
        );
        assert_eq!(
            storage
                .last_block(TEST_CHAIN_ID_MAINNET, &erc721_transfer_event())
                .await?,
            highest_block_erc721_transfer
        );

        Ok(())
    }

    /// Check that the latest block is properly updated when calling the synchronize_events method.
    #[sqlx::test(migrations = "./migrations")]
    async fn synchronize_events(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;
        let latest_block = Some(Faker.fake::<u64>());

        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_approval()
            .with_erc721_transfer()
            .build();

        storage
            .include_events(
                TEST_CHAIN_ID_MAINNET,
                &[approval_event(), erc721_transfer_event()],
            )
            .await?;

        // We add some logs
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // But now, we tell the indexer that we have indexed events up to the latest block
        storage
            .synchronize_events(
                TEST_CHAIN_ID_MAINNET,
                &[
                    approval_event().selector(),
                    erc721_transfer_event().selector(),
                ],
                latest_block,
            )
            .await?;

        // So instead of having the latest block be the highest block in the logs, it should be the latest
        //block that we told the indexer.
        assert_eq!(
            storage
                .last_block(TEST_CHAIN_ID_MAINNET, &approval_event())
                .await?,
            latest_block.unwrap()
        );
        assert_eq!(
            storage
                .last_block(TEST_CHAIN_ID_MAINNET, &erc721_transfer_event())
                .await?,
            latest_block.unwrap()
        );

        Ok(())
    }

    /// Test case for the strict mode logic.
    #[sqlx::test(migrations = "./migrations")]
    async fn strict_mode(pool: Pool<Postgres>) -> Result<()> {
        let mut storage = PostgreSqlStorage::new(pool.clone());

        // Let's start inserting some logs into the DB that should pass through
        storage.set_strict_mode(true);

        // The indexer is expected to receive only transfer events
        let transfer_event = transfer_event();

        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event.clone()])
            .await?;

        // Build some logs that only contain transfer events

        // Add the logs
        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize; // number between 1 and 50
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_transfer()
            .build();
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        assert_eq!(
            event_count_from_db(&pool, TEST_CHAIN_ID_MAINNET, &transfer_event).await?,
            num_logs
        );

        // Now, let's verify that logs containing approval events are rejected
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_approval()
            .build();
        assert!(
            storage
                .add_events(TEST_CHAIN_ID_MAINNET, &logs)
                .await
                .is_err(),
            "strict mode should reject unknown events"
        );

        // Finally, let's verify that logs containing a mix of events insert the known and ignore the
        // unknown ones when running in strict mode = `false`
        storage.set_strict_mode(false);
        let mixed_log_count = (Faker.fake::<u8>() % 50 + 1) as usize;
        let logs = LogTestFixture::builder()
            .with_log_count(mixed_log_count)
            .with_erc20_transfer()
            .with_erc20_approval()
            .build();
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // The LogTestFixture cycles through event kinds, so with 2 event types,
        // indices 0, 2, 4, ... are transfers. Count them.
        let transfer_count_in_mixed = (mixed_log_count + 1) / 2;

        // The former call should not return an error, so if we get here, it means the unknown event was ignored.
        // The total count should be: num_logs (from first insertion) + transfer_count_in_mixed (from mixed insertion)
        assert_eq!(
            event_count_from_db(&pool, TEST_CHAIN_ID_MAINNET, &transfer_event).await?,
            num_logs + transfer_count_in_mixed
        );

        Ok(())
    }

    /// Test case for listing all indexed events.
    #[sqlx::test(migrations = "./migrations")]
    async fn list_indexed_events(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Include events for two different chains
        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event(), approval_event()])
            .await?;
        storage
            .include_events(TEST_CHAIN_ID_BASE, &[erc721_transfer_event()])
            .await?;

        // Add some logs to have event counts
        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_transfer()
            .build();
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // List all indexed events
        let events = storage.list_indexed_events().await?;

        // We should have 3 event descriptors (2 on mainnet + 1 on base)
        assert_eq!(events.len(), 3, "should have 3 indexed events");

        // Check that the Transfer event on mainnet has the correct count
        let transfer_mainnet = events.iter().find(|e| {
            e.chain_id == Some(TEST_CHAIN_ID_MAINNET) && e.event_name.as_deref() == Some("Transfer")
        });
        assert!(
            transfer_mainnet.is_some(),
            "Transfer event on mainnet should exist"
        );
        assert_eq!(
            transfer_mainnet.unwrap().event_count,
            Some(num_logs),
            "Transfer event count should match"
        );

        // Approval event should have 0 count (no logs added)
        let approval_mainnet = events.iter().find(|e| {
            e.chain_id == Some(TEST_CHAIN_ID_MAINNET) && e.event_name.as_deref() == Some("Approval")
        });
        assert!(
            approval_mainnet.is_some(),
            "Approval event on mainnet should exist"
        );
        assert_eq!(
            approval_mainnet.unwrap().event_count,
            Some(0),
            "Approval event count should be 0"
        );

        // ERC721 Transfer on Base should exist with 0 count
        let transfer_base = events
            .iter()
            .find(|e| e.chain_id == Some(TEST_CHAIN_ID_BASE));
        assert!(
            transfer_base.is_some(),
            "Transfer event on Base should exist"
        );
        assert_eq!(
            transfer_base.unwrap().event_count,
            Some(0),
            "ERC721 Transfer event count should be 0"
        );

        Ok(())
    }

    /// Test case for sending raw SELECT queries.
    #[sqlx::test(migrations = "./migrations")]
    async fn send_raw_query(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Include and add some events first
        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event()])
            .await?;

        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_transfer()
            .build();
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // Test a valid SELECT query
        let selector = transfer_event().selector().to_string();
        let short_hash = &selector[2..7];
        let query = format!(
            "SELECT COUNT(*) as cnt FROM event_{}_transfer_{}",
            TEST_CHAIN_ID_MAINNET, short_hash
        );
        let result = storage.send_raw_query(&query).await?;

        // Result should be an array with one object containing the count
        assert!(result.is_array(), "result should be an array");
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1, "should have one row");
        // COUNT(*) returns INT8 which is converted to a JSON Number
        assert_eq!(
            arr[0]["cnt"].as_i64(),
            Some(num_logs as i64),
            "count should match number of logs"
        );

        // Test that non-SELECT queries are rejected
        let delete_query = "DELETE FROM event_descriptor";
        let result = storage.send_raw_query(delete_query).await?;
        assert!(
            result.get("error").is_some(),
            "non-SELECT queries should return an error"
        );

        let insert_query = "INSERT INTO event_descriptor (chain_id) VALUES (999)";
        let result = storage.send_raw_query(insert_query).await?;
        assert!(
            result.get("error").is_some(),
            "INSERT queries should return an error"
        );

        Ok(())
    }

    /// Test case for describing the database schema.
    #[sqlx::test(migrations = "./migrations")]
    async fn describe_database(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Include some events to create tables
        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event(), approval_event()])
            .await?;

        // Describe the database
        let schema = storage.describe_database().await?;

        // Result should be an array of table definitions
        assert!(schema.is_array(), "schema should be an array");
        let tables = schema.as_array().unwrap();

        // We should have at least: event_descriptor, blocks_1, event_1_transfer_*, event_1_approval_*
        assert!(tables.len() >= 4, "should have at least 4 tables");

        // Find the event_descriptor table
        let event_descriptor_table = tables.iter().find(|t| {
            t.as_object()
                .map(|obj| obj.contains_key("event_descriptor"))
                .unwrap_or(false)
        });
        assert!(
            event_descriptor_table.is_some(),
            "event_descriptor table should exist"
        );

        // Check that event_descriptor has expected columns
        let ed_columns = event_descriptor_table
            .unwrap()
            .get("event_descriptor")
            .and_then(|v| v.as_object());
        assert!(ed_columns.is_some(), "event_descriptor should have columns");
        let columns = ed_columns.unwrap();
        assert!(
            columns.contains_key("chain_id"),
            "should have chain_id column"
        );
        assert!(
            columns.contains_key("event_hash"),
            "should have event_hash column"
        );
        assert!(
            columns.contains_key("event_name"),
            "should have event_name column"
        );

        // Find a blocks table
        let blocks_table = tables.iter().find(|t| {
            t.as_object()
                .map(|obj| obj.keys().any(|k| k.starts_with("blocks_")))
                .unwrap_or(false)
        });
        assert!(blocks_table.is_some(), "blocks table should exist");

        Ok(())
    }

    /// Test case for listing contracts from event tables.
    #[sqlx::test(migrations = "./migrations")]
    async fn list_contracts(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Include events
        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event()])
            .await?;

        // Add logs with specific contract addresses
        let contract1 = fake_address();
        let contract2 = fake_address();
        let num_logs = (Faker.fake::<u8>() % 50 + 1) as usize;
        let logs = LogTestFixture::builder()
            .with_log_count(num_logs)
            .with_erc20_transfer()
            .with_contract_addresses([contract1.clone(), contract2.clone()])
            .build();
        storage.add_events(TEST_CHAIN_ID_MAINNET, &logs).await?;

        // List contracts
        let contracts = storage.list_contracts().await?;

        // Should have at least our 2 contracts
        assert!(contracts.len() >= 2, "should have at least 2 contracts");

        // Check that our contracts are in the list
        let addresses: Vec<&str> = contracts
            .iter()
            .map(|c| c.contract_address.as_str())
            .collect();
        assert!(
            addresses.contains(&contract1.to_lowercase().as_str())
                || addresses.iter().any(|a| a.eq_ignore_ascii_case(&contract1)),
            "contract1 should be in the list"
        );
        assert!(
            addresses.contains(&contract2.to_lowercase().as_str())
                || addresses.iter().any(|a| a.eq_ignore_ascii_case(&contract2)),
            "contract2 should be in the list"
        );

        Ok(())
    }

    /// Test case for getting event signatures by hash.
    #[sqlx::test(migrations = "./migrations")]
    async fn get_event_signature(pool: Pool<Postgres>) -> Result<()> {
        let storage = PostgreSqlStorage::new(pool.clone());

        // Include events to register them in the event_descriptor table
        storage
            .include_events(TEST_CHAIN_ID_MAINNET, &[transfer_event(), approval_event()])
            .await?;

        // Test get_event_signature with 0x prefix
        let event_hash = transfer_event().selector().to_string();
        let signature = storage.get_event_signature(&event_hash).await?;

        // The signature should match the full signature of the Transfer event
        assert_eq!(
            signature,
            transfer_event().full_signature(),
            "event signature should match"
        );

        // Test without the 0x prefix
        let event_hash_no_prefix = event_hash.trim_start_matches("0x");
        let signature2 = storage.get_event_signature(event_hash_no_prefix).await?;
        assert_eq!(
            signature2,
            transfer_event().full_signature(),
            "event signature should match without 0x prefix"
        );

        // Test with a different event (Approval)
        let approval_hash = approval_event().selector().to_string();
        let approval_signature = storage.get_event_signature(&approval_hash).await?;
        assert_eq!(
            approval_signature,
            approval_event().full_signature(),
            "approval event signature should match"
        );

        Ok(())
    }
}

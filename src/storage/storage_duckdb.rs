// Copyright (c) 2026 Bilinear Labs
// SPDX-License-Identifier: MIT

//! Module that handles the connection to the DuckDB database.

use crate::{
    EventStatus,
    constants::*,
    error_codes::ERROR_CODE_DATABASE_LOCKED,
    storage::{ContractDescriptorDb, EventDescriptorDb, Storage},
};
use alloy::{
    dyn_abi::{DecodedEvent, DynSolValue, EventExt},
    json_abi::Event,
    primitives::B256,
    rpc::types::Log,
};
use anyhow::{Context, Result};
use duckdb::{Connection, OptionalExt, types::ValueRef};
use serde_json::{Map, Number, Value, json};
use std::{
    collections::{HashMap, HashSet},
    string::ToString,
    sync::{Mutex, RwLock},
};
use tracing::{debug, error, info, warn};

/// Implementation of the Storage trait for the DuckDB database.
///
/// # Description
///
/// The DuckDBStorage object is responsible for the connection to the DuckDB database. It provides a thread-safe
/// interface for the storage of the indexed data.
///
/// The logic can run in strict mode, which means that the indexing will stop if an event fails to be processed.
pub struct DuckDBStorage {
    conn: Mutex<Connection>,
    db_path: String,
    strict_mode: bool,
    event_descriptors: RwLock<HashMap<String, Event>>,
}

/// Simple factory pattern to allow opening a new connection to the same database from a task.
///
/// # Description
///
/// The main purpose of this object is to allow opening concurrent connections for reading from the database.
/// The main use case is the REST API, which needs to open a new connection for each request. Using this entry
/// point, we avoid lock contention.
#[derive(Clone)]
pub struct DuckDBStorageFactory {
    db_path: String,
}

impl DuckDBStorageFactory {
    pub fn new(db_path: String) -> Self {
        Self { db_path }
    }

    pub fn create(&self) -> Result<DuckDBStorage> {
        DuckDBStorage::with_db(&self.db_path)
    }
}

impl Clone for DuckDBStorage {
    /// Cloning a DuckDBStorage object will open a new concurrent connection to the same database.
    fn clone(&self) -> Self {
        DuckDBStorage::with_db(&self.db_path).unwrap()
    }
}

impl Storage for DuckDBStorage {
    fn add_events(&self, chain_id: u64, events: &[Log]) -> Result<()> {
        // Quick check to avoid unnecessary operations.
        if events.is_empty() {
            info!("No events for the given block range");
            return Ok(());
        }

        let mut conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let tx = conn.transaction()?;

        // First stage: insert the new blocks into the chain-specific blocks table.
        // Use INSERT ... ON CONFLICT DO NOTHING to ignore duplicates without aborting the transaction.
        // Batch multiple rows into a single INSERT for better performance.
        {
            // Collect unique blocks first
            let mut blocks_to_insert: Vec<(String, String, String)> = Vec::new();
            let mut last_block_number = 0;
            for event in events {
                if let Some(block_number) = event.block_number
                    && block_number != last_block_number
                {
                    blocks_to_insert.push((
                        block_number.to_string(),
                        event.block_hash.unwrap().to_string(),
                        event.block_timestamp.unwrap_or_default().to_string(),
                    ));
                    last_block_number = block_number;
                }
            }

            // Batch insert in chunks for better performance
            let blocks_table = format!("blocks_{}", chain_id);

            for chunk in blocks_to_insert.chunks(DUCKDB_BATCH_INSERT_SIZE) {
                if chunk.is_empty() {
                    continue;
                }

                // Build VALUES clause: (?, ?, ?), (?, ?, ?), ...
                let values_placeholders: Vec<&str> = chunk.iter().map(|_| "(?, ?, ?)").collect();
                let insert_sql = format!(
                    "INSERT INTO {} (block_number, block_hash, block_timestamp) VALUES {} ON CONFLICT DO NOTHING",
                    blocks_table,
                    values_placeholders.join(", ")
                );

                // Flatten parameters
                let params: Vec<&dyn duckdb::ToSql> = chunk
                    .iter()
                    .flat_map(|(bn, bh, bt)| {
                        vec![
                            bn as &dyn duckdb::ToSql,
                            bh as &dyn duckdb::ToSql,
                            bt as &dyn duckdb::ToSql,
                        ]
                    })
                    .collect();

                tx.execute(&insert_sql, params.as_slice())?;
            }
        }

        // Second stage: insert the new events into the event_X table.

        // Group events by table name for bulk insertion using appenders
        let mut events_by_table: HashMap<String, Vec<&Log>> = HashMap::new();

        // First pass: filter and group events by table
        for log in events {
            if log.block_number.is_none()
                || log.topic0().is_none()
                || log.transaction_hash.is_none()
            {
                continue;
            }

            // Retrieve the event's signature to get the event's name provided the event's hash from the Log.
            let event_hash = log.topic0().unwrap().to_string();

            // Build the key for the local cache: chain_id:short_hash
            let short_hash = Self::short_hash(&event_hash);
            let cache_key = format!("{}:{}", chain_id, short_hash);

            // Parse the received events (as hashes) into event descriptors using the local cache of known events.
            // Only known events by the indexer can be processed. Thus if the indexer is running in strict mode, the
            // indexing will stop if the event descriptor is not found.
            let event = match self
                .event_descriptors
                .read()
                .map_err(|_| {
                    anyhow::anyhow!("Failed to acquire read lock for the event descriptor")
                })?
                .get(&cache_key)
            {
                Some(event) => event.clone(),
                None => {
                    if self.strict_mode {
                        return Err(anyhow::anyhow!(
                            "The event {event_hash} on chain {chain_id:#x} was received but the event descriptor was not found. You can either include the event in the command line arguments or run the indexer with the strict mode disabled."
                        ));
                    } else {
                        warn!(
                            "The event {event_hash} on chain {chain_id:#x} was received but the event descriptor was not found. This event will be ignored."
                        );
                        continue;
                    }
                }
            };

            let table_name = format!(
                "event_{}_{}_{}",
                chain_id,
                event.name.as_str().to_ascii_lowercase(),
                Self::short_hash(&event_hash)
            );
            events_by_table
                .entry(table_name)
                .or_insert_with(Vec::new)
                .push(log);
        }

        // Second pass: process each table's events in bulk
        for (table_name, table_events) in events_by_table {
            if table_events.is_empty() {
                continue;
            }

            // Get short_hash from table name: event_<chain_id>_<name>_<short_hash>
            let short_hash = table_name.split("_").nth(3).unwrap();

            // Get the full event hash from the first log in the table (for UPDATE statement)
            let event_hash = table_events
                .first()
                .and_then(|log| log.topic0())
                .map(|h| h.to_string())
                .unwrap_or_default();

            // Build the cache key: chain_id:short_hash
            let cache_key = format!("{}:{}", chain_id, short_hash);

            // Get the parsed event from the event descriptors
            let parsed_event = self
                .event_descriptors
                .read()
                .map_err(|_| anyhow::anyhow!("Failed to acquire read lock for the event descriptor"))?
                .get(&cache_key)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Event descriptor not found for chain {:#x} hash: {event_hash}. Ensure the event is registered before processing.",
                        chain_id
                    )
                })?
                .clone();

            let mut appender = tx.appender(&table_name)?;

            for log in table_events {
                // Insert the always present fields (chain_id is encoded in table name)
                let mut row_vals: Vec<String> = vec![
                    log.block_number.unwrap().to_string(),
                    log.transaction_hash.unwrap().to_string(),
                    log.log_index.unwrap().to_string(),
                    log.address().to_string(),
                ];

                // Parse the indexed and non-indexed parameters and convert them to strings ready for the DB.
                if let Ok(DecodedEvent { indexed, body, .. }) =
                    parsed_event.decode_log_parts(log.topics().to_vec(), log.data().data.as_ref())
                {
                    for item in indexed {
                        row_vals.push(Self::dyn_sol_value_to_string(&item));
                    }
                    for item in body {
                        row_vals.push(Self::dyn_sol_value_to_string(&item));
                    }
                }

                appender.append_row(duckdb::appender_params_from_iter(
                    row_vals.iter().map(|s| s.as_str()),
                ))?;

                tx.execute(
                    "UPDATE event_descriptor SET last_block = ? WHERE chain_id = ? AND event_hash = ?",
                    [
                        log.block_number.unwrap().to_string(),
                        chain_id.to_string(),
                        event_hash.clone(),
                    ],
                )?;
            }

            // Flush the appender for this table
            appender.flush()?;
        }

        // Explicitly commit the transaction
        tx.commit()?;

        Ok(())
    }

    fn list_indexed_events(&self) -> Result<Vec<EventDescriptorDb>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut rows = conn.prepare(
            "SELECT chain_id, event_hash, event_signature, event_name, first_block, last_block FROM event_descriptor",
        )?;
        let mut events = rows
            .query_map([], |row| {
                Ok(EventDescriptorDb {
                    chain_id: row.get(0).optional()?,
                    event_hash: row.get(1).optional()?,
                    event_signature: row.get(2).optional()?,
                    event_name: row.get(3).optional()?,
                    first_block: row.get(4).optional()?,
                    last_block: row.get(5).optional()?,
                    event_count: None,
                })
            })?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<EventDescriptorDb>>>()?;

        // Now, let's populate the event count
        for event in events.iter_mut() {
            let chain_id = event.chain_id.unwrap();
            let short_hash = Self::short_hash(event.event_hash.as_ref().unwrap());
            let table_name = format!(
                "event_{}_{}_{}",
                chain_id,
                event.event_name.as_ref().unwrap().to_ascii_lowercase(),
                short_hash
            );

            let event_count: usize =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |row| {
                    row.get(0)
                })?;
            event.event_count = Some(event_count);
        }

        Ok(events)
    }

    #[inline]
    fn last_block(&self, chain_id: u64, event: &Event) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            "SELECT last_block FROM event_descriptor WHERE chain_id = ? AND event_hash = ?",
            [chain_id.to_string(), event.selector().to_string()],
            |row| row.get(0),
        )?)
    }

    #[inline]
    fn first_block(&self, chain_id: u64, event: &Event) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            "SELECT first_block FROM event_descriptor WHERE chain_id = ? AND event_hash = ?",
            [chain_id.to_string(), event.selector().to_string()],
            |row| row.get(0),
        )?)
    }

    fn include_events(&self, chain_id: u64, events: &[Event]) -> Result<()> {
        DuckDBStorage::create_event_schema(&self.conn, chain_id, events)?;
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
    }

    fn get_event_signature(&self, event_hash: &str) -> Result<String> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            "SELECT event_signature FROM event_descriptor WHERE event_hash = ?",
            [event_hash],
            |row| row.get(0),
        )?)
    }

    fn event_index_status(&self, chain_id: u64, event: &Event) -> Result<Option<EventStatus>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        let Some((mut event_status, event_name)) = conn
            .query_row(
                "SELECT \"event_name\", \"first_block\", \"last_block\" FROM event_descriptor WHERE chain_id = ? AND event_hash = ?",
                [chain_id.to_string(), event.selector().to_string()],
                |row| {
                    Ok((EventStatus {
                        hash: event.selector().to_string(),
                        first_block: row.get(1)?,
                        last_block: row.get(2)?,
                        event_count: 0,
                    }, row.get::<_, String>(0)?))
                },
            ).optional()? else {
                return Ok(None);
            };

        if event_status.last_block != 0 {
            // Get the event count from the chain-specific table
            let event_count: usize = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM event_{}_{}_{}",
                    chain_id,
                    event_name.to_ascii_lowercase(),
                    Self::short_hash(&event.selector().to_string())
                ),
                [],
                |row| row.get(0),
            )?;

            event_status.event_count = event_count;
        }

        Ok(Some(event_status))
    }

    fn synchronize_events(
        &self,
        chain_id: u64,
        event_selectors: &[B256],
        last_processed: Option<u64>,
    ) -> Result<()> {
        if event_selectors.is_empty() {
            debug!(
                "No event selectors provided for synchronize_events on chain {:#x}, skipping",
                chain_id
            );
            return Ok(());
        }

        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        // Build the IN clause with placeholders for each event selector
        let placeholders: Vec<&str> = event_selectors.iter().map(|_| "?").collect();
        let in_clause = placeholders.join(", ");

        if let Some(last_processed) = last_processed {
            let query = format!(
                "UPDATE event_descriptor SET last_block = ? WHERE chain_id = ? AND event_hash IN ({})",
                in_clause
            );

            // Build parameters: [last_processed, chain_id, selector1, selector2, ...]
            let mut params: Vec<String> = vec![last_processed.to_string(), chain_id.to_string()];
            params.extend(event_selectors.iter().map(|s| s.to_string()));

            conn.execute(&query, duckdb::params_from_iter(params))?;
        } else {
            // When no last_processed is given, use the max last_block among the specified events
            let query = format!(
                "UPDATE event_descriptor SET last_block = (SELECT MAX(last_block) FROM event_descriptor WHERE chain_id = ? AND event_hash IN ({})) WHERE chain_id = ? AND event_hash IN ({})",
                in_clause, in_clause
            );

            // Build parameters: [chain_id, selectors..., chain_id, selectors...]
            let mut params: Vec<String> = vec![chain_id.to_string()];
            params.extend(event_selectors.iter().map(|s| s.to_string()));
            params.push(chain_id.to_string());
            params.extend(event_selectors.iter().map(|s| s.to_string()));

            conn.execute(&query, duckdb::params_from_iter(params))?;
        }

        debug!(
            "Events synchronized up to block {}",
            last_processed.unwrap_or_default()
        );

        Ok(())
    }

    fn send_raw_query(&self, query: &str) -> Result<Value> {
        if !query.trim_start().to_uppercase().starts_with("SELECT") {
            return Ok(json!({ "error": "Query must be a SELECT statement" }));
        }

        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        // First, try to get column names using DESCRIBE
        let mut column_names = Self::get_column_names_from_query(&conn, query)?;

        // Execute the actual query
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        // If we couldn't get column names from DESCRIBE, infer from first row
        if column_names.is_empty() {
            if let Some(first_row) = rows.next()? {
                column_names = Self::infer_column_count_and_names(first_row)?;

                // Process the first row
                let mut result_obj = Map::new();
                for (i, col_name) in column_names.iter().enumerate() {
                    let value = Self::infer_value_type(first_row, i)?;
                    result_obj.insert(col_name.clone(), value);
                }
                let mut results = vec![Value::Object(result_obj)];

                // Process remaining rows
                while let Some(row) = rows.next()? {
                    let mut result_obj = Map::new();
                    for (i, col_name) in column_names.iter().enumerate() {
                        let value = Self::infer_value_type(row, i)?;
                        result_obj.insert(col_name.clone(), value);
                    }
                    results.push(Value::Object(result_obj));
                }

                return Ok(Value::Array(results));
            } else {
                // Empty result set
                return Ok(Value::Array(vec![]));
            }
        }

        // We have column names, process all rows
        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            let mut result_obj = Map::new();
            for (i, col_name) in column_names.iter().enumerate() {
                let value = Self::infer_value_type(row, i)?;
                result_obj.insert(col_name.clone(), value);
            }
            results.push(Value::Object(result_obj));
        }

        Ok(Value::Array(results))
    }

    fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        // First let's find out how many contracts are indexed.
        // For each event table, a contract is listed.
        let mut statement =
            conn.prepare("SELECT chain_id, event_hash, event_name FROM event_descriptor")?;

        let event_descriptors = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .map(|r| r.unwrap())
            .collect::<Vec<(u64, String, String)>>();

        let mut contracts = HashSet::new();
        for (chain_id, event_hash, event_name) in event_descriptors {
            let table_name = format!(
                "event_{}_{}_{}",
                chain_id,
                event_name.to_ascii_lowercase(),
                Self::short_hash(&event_hash)
            );

            let mut statement = conn.prepare(&format!(
                "SELECT DISTINCT contract_address FROM {table_name}"
            ))?;

            let addresses = statement
                .query_map([], |row| row.get(0))?
                .map(|r| r.unwrap())
                .collect::<Vec<String>>();

            contracts.extend(addresses.into_iter());
        }

        Ok(contracts
            .into_iter()
            .map(|address| ContractDescriptorDb {
                contract_address: address,
                contract_name: None,
            })
            .collect::<Vec<ContractDescriptorDb>>())
    }
}

impl DuckDBStorage {
    /// Length of the short hash used in table names (5 hex characters).
    const SHORT_HASH_LEN: usize = 5;

    /// Returns a shortened version of an event hash for use in table names.
    ///
    /// Takes the first 5 hex characters after the "0x" prefix.
    /// Example: "0xddf252ad..." -> "ddf25"
    #[inline]
    fn short_hash(full_hash: &str) -> &str {
        // Skip "0x" prefix and take first 5 characters
        let start = if full_hash.starts_with("0x") { 2 } else { 0 };
        &full_hash[start..start + Self::SHORT_HASH_LEN]
    }

    pub fn new() -> Result<DuckDBStorage> {
        Self::with_db(DUCKDB_FILE_PATH)
    }

    /// Creates a new DuckDBStorage with the given database path.
    pub fn with_db(db_path: &str) -> Result<DuckDBStorage> {
        let conn = if let Ok(conn) = Connection::open(db_path) {
            conn
        } else {
            error!(
                "Failed to open database: {db_path}. Check that the DB file is not locked by another process."
            );
            std::process::exit(ERROR_CODE_DATABASE_LOCKED);
        };

        let table_exists: bool = conn.query_row(
            r#"
                SELECT
                    count(*)
                FROM
                    information_schema.tables
                WHERE
                    table_schema = 'main'
                    AND table_name = ?
                    AND table_type = 'BASE TABLE';"#,
            [DUCKDB_BASE_TABLE_NAME],
            |row| row.get(0),
        )?;

        let conn_mutex = if !table_exists {
            let conn_mutex = Mutex::new(conn);
            DuckDBStorage::create_db_base(&conn_mutex)?;
            conn_mutex
        } else {
            // Try to retrieve version from quixote_info table using a query
            let version: String = conn
                .query_row(
                    format!("SELECT version FROM {DUCKDB_BASE_TABLE_NAME} LIMIT 1").as_str(),
                    [],
                    |row| row.get(0),
                )
                .with_context(|| {
                    format!("Failed to retrieve version from {DUCKDB_BASE_TABLE_NAME} table")
                })?;

            if version != DUCKDB_SCHEMA_VERSION {
                warn!("Your database is out of date. Please run the database upgrade.");
            }
            Mutex::new(conn)
        };

        debug!("Database connection successfully established");

        Ok(DuckDBStorage {
            conn: conn_mutex,
            db_path: db_path.to_string(),
            strict_mode: false,
            event_descriptors: RwLock::new(HashMap::new()),
        })
    }

    fn create_db_base(conn: &Mutex<Connection>) -> Result<()> {
        // Note: blocks tables are created per-chain dynamically (blocks_<chain_id>)
        // Event tables are also created per-chain dynamically (event_<chain_id>_<name>_<hash>)
        let statement = format!(
            "
            BEGIN;
            CREATE TABLE IF NOT EXISTS {DUCKDB_BASE_TABLE_NAME}(
                version VARCHAR NOT NULL,
                PRIMARY KEY (version)
            );
            CREATE TABLE IF NOT EXISTS event_descriptor(
                chain_id UBIGINT NOT NULL,
                event_hash VARCHAR(66) NOT NULL,
                event_signature VARCHAR(256) NOT NULL,
                event_name VARCHAR(40) NOT NULL,
                first_block UBIGINT,
                last_block UBIGINT,
                PRIMARY KEY (chain_id, event_hash)
            );
            COMMIT;"
        );
        {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
            conn.execute_batch(&statement)?;
        }

        {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
            conn.execute(
                &format!(
                    "INSERT INTO {} (\"version\")
                VALUES (?);",
                    DUCKDB_BASE_TABLE_NAME
                ),
                [DUCKDB_SCHEMA_VERSION],
            )?;
        }

        Ok(())
    }

    fn create_event_schema(
        conn: &Mutex<Connection>,
        chain_id: u64,
        events: &[Event],
    ) -> Result<()> {
        // First, ensure the blocks table for this chain exists
        {
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
            conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS blocks_{chain_id}(
                        block_number UBIGINT NOT NULL,
                        block_hash VARCHAR(66) NOT NULL,
                        block_timestamp UBIGINT NOT NULL,
                        PRIMARY KEY (block_number)
                    )"
                ),
                [],
            )?;
        }

        for event in events {
            // Table name format: event_<chain_id>_<name>_<short_hash>
            let event_hash = B256::from(event.selector()).to_string();
            let short_hash = Self::short_hash(&event_hash);
            let event_name = event.name.as_str().to_ascii_lowercase();

            debug!(
                "Creating event schema for chain {:#x}: {}",
                chain_id,
                event.full_signature()
            );

            // Build the table definition (no chain_id column needed - it's in the table name)
            let mut statement = format!(
                "
                    CREATE TABLE IF NOT EXISTS event_{chain_id}_{event_name}_{short_hash}(
                        block_number UBIGINT NOT NULL,
                        transaction_hash VARCHAR NOT NULL,
                        log_index USMALLINT NOT NULL,
                        contract_address VARCHAR NOT NULL,
                ",
            );

            event.inputs.iter().for_each(|param| {
                let db_type = if param.selector_type() == "uint256" {
                    "VARINT"
                } else {
                    "VARCHAR"
                };
                statement.push_str(&format!("\"{}\" {db_type},", param.name));
            });

            statement.push_str("PRIMARY KEY (block_number, transaction_hash, log_index));");

            // Create an entry for this event in the event_descriptor table.
            statement.push_str(&format!(
                "INSERT INTO event_descriptor (chain_id, event_hash, event_signature, event_name, first_block, last_block) VALUES ({}, '{}', '{}', '{}', 0, 0) ON CONFLICT (chain_id, event_hash) DO NOTHING;",
                chain_id, event.selector(), event.full_signature(), event.name
            ));

            // Not a big deal to batch this SQL statement as it is executed once during the apps's lifetime.
            let conn = conn
                .lock()
                .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
            conn.execute(&statement, [])?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    #[inline]
    fn update_last_block(&self, block_number: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        conn.execute(
            &format!("UPDATE {DUCKDB_BASE_TABLE_NAME} SET last_block = ?"),
            [block_number.to_string()],
        )?;
        Ok(())
    }

    #[inline]
    pub fn set_first_block(&self, chain_id: u64, event: &Event, block_number: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        conn.execute(
            "UPDATE event_descriptor SET first_block = ? WHERE chain_id = ? AND event_hash = ?",
            [
                block_number.to_string(),
                chain_id.to_string(),
                event.selector().to_string(),
            ],
        )?;
        Ok(())
    }

    /// Attempts to infer the JSON value type for a column in a row.
    ///
    /// # Description
    ///
    /// Uses ValueRef to directly access the underlying value and convert to JSON.
    /// Numeric types that can overflow JSON's number representation (like BIGNUM,
    /// HugeInt, Decimal) are converted to strings to preserve precision.
    fn infer_value_type(row: &duckdb::Row, col_idx: usize) -> Result<Value> {
        // Use get_ref to access the raw ValueRef
        // If get_ref fails (e.g., for unsupported types like BIGNUM),
        // fall back to string extraction
        let value_ref = match row.get_ref(col_idx) {
            Ok(v) => v,
            Err(_) => {
                // Fallback: try to get as String for unsupported types (like BIGNUM)
                if let Ok(s) = row.get::<_, String>(col_idx) {
                    return Ok(Value::String(s));
                }
                return Ok(Value::Null);
            }
        };

        match value_ref {
            ValueRef::Null => Ok(Value::Null),
            ValueRef::Boolean(b) => Ok(Value::Bool(b)),
            ValueRef::TinyInt(i) => Ok(Value::Number(Number::from(i))),
            ValueRef::SmallInt(i) => Ok(Value::Number(Number::from(i))),
            ValueRef::Int(i) => Ok(Value::Number(Number::from(i))),
            ValueRef::BigInt(i) => Ok(Value::Number(Number::from(i))),
            ValueRef::HugeInt(i) => {
                // HugeInt (i128) - convert to string to preserve precision
                Ok(Value::String(i.to_string()))
            }
            ValueRef::UTinyInt(u) => Ok(Value::Number(Number::from(u))),
            ValueRef::USmallInt(u) => Ok(Value::Number(Number::from(u))),
            ValueRef::UInt(u) => Ok(Value::Number(Number::from(u))),
            ValueRef::UBigInt(u) => {
                // u64 is directly supported by serde_json::Number
                Ok(Value::Number(Number::from(u)))
            }
            ValueRef::Float(f) => {
                if let Some(num) = Number::from_f64(f as f64) {
                    Ok(Value::Number(num))
                } else {
                    Ok(Value::String(f.to_string()))
                }
            }
            ValueRef::Double(d) => {
                if let Some(num) = Number::from_f64(d) {
                    Ok(Value::Number(num))
                } else {
                    Ok(Value::String(d.to_string()))
                }
            }
            ValueRef::Decimal(d) => {
                // Decimal (used for BIGNUM) - convert to string to preserve precision
                Ok(Value::String(d.to_string()))
            }
            ValueRef::Timestamp(_, ts) => Ok(Value::Number(Number::from(ts))),
            ValueRef::Text(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                Ok(Value::String(s.into_owned()))
            }
            ValueRef::Blob(_) => {
                // For Blob types (including BIGNUM which is exposed as Blob),
                // try to parse as BIGNUM first, otherwise hex encode
                if let Ok(bytes) = row.get::<_, Vec<u8>>(col_idx) {
                    // Try to parse as BIGNUM (format: [flag][is_negative][size][data...])
                    if let Some(hex_str) = Self::try_parse_bignum_blob(&bytes) {
                        Ok(Value::String(hex_str))
                    } else {
                        // Regular blob - hex encode
                        Ok(Value::String(format!("0x{}", hex::encode(bytes))))
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            ValueRef::Date32(d) => Ok(Value::Number(Number::from(d))),
            ValueRef::Time64(_, t) => Ok(Value::Number(Number::from(t))),
            ValueRef::Interval {
                months,
                days,
                nanos,
            } => {
                // Return interval as a JSON object
                let mut map = Map::new();
                map.insert("months".to_string(), Value::Number(Number::from(months)));
                map.insert("days".to_string(), Value::Number(Number::from(days)));
                map.insert("nanos".to_string(), Value::Number(Number::from(nanos)));
                Ok(Value::Object(map))
            }
            // For complex types (List, Enum, Struct, Array, Map, Union),
            // fall back to string representation via row.get
            _ => {
                // Try to get as String as last resort
                if let Ok(s) = row.get::<_, String>(col_idx) {
                    Ok(Value::String(s))
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    /// Tries to parse a blob as a DuckDB BIGNUM and convert to hex string.
    ///
    /// BIGNUM blob format: [flag][is_negative][size][data in big-endian]
    /// Example: 0x800020ff...ff (flag=0x80, is_negative=0x00, size=0x20=32 bytes, data=ff...ff)
    fn try_parse_bignum_blob(bytes: &[u8]) -> Option<String> {
        // BIGNUM blobs have at least 3 bytes header
        if bytes.len() < 3 {
            return None;
        }

        // Check for BIGNUM signature (flag byte is 0x80)
        if bytes[0] != 0x80 {
            return None;
        }

        let is_negative = bytes[1] != 0;
        let size = bytes[2] as usize;

        // Validate that we have enough bytes for the data
        if bytes.len() != 3 + size {
            return None;
        }

        let data = &bytes[3..];

        // Data is already in big-endian format, just encode as hex
        let hex_str = hex::encode(data);

        // Handle negative numbers (rare for uint256, but supported by BIGNUM)
        if is_negative && hex_str != "0" && hex_str != "00" {
            Some(format!("-0x{}", hex_str))
        } else {
            Some(format!("0x{}", hex_str))
        }
    }

    /// Gets column names from a query by using DESCRIBE on a subquery.
    fn get_column_names_from_query(conn: &Connection, query: &str) -> Result<Vec<String>> {
        // Use DESCRIBE to get column information from the query result
        // Wrap the query in a subquery to make DESCRIBE work
        let describe_query = format!("DESCRIBE SELECT * FROM ({}) LIMIT 0", query);

        match conn.prepare(&describe_query) {
            Ok(mut stmt) => {
                match stmt.query([]) {
                    Ok(mut rows) => {
                        let mut column_names = Vec::new();
                        while let Some(row) = rows.next()? {
                            // DESCRIBE returns: column_name, column_type, null, key, default, extra
                            // We only need the first column (column_name)
                            match row.get::<_, String>(0) {
                                Ok(col_name) => column_names.push(col_name),
                                Err(_) => {
                                    // If we can't get the name, use a generic one
                                    column_names.push(format!("column_{}", column_names.len()));
                                }
                            }
                        }
                        Ok(column_names)
                    }
                    Err(_) => {
                        // If DESCRIBE fails, return empty vec - we'll infer from first row
                        Ok(Vec::new())
                    }
                }
            }
            Err(_) => {
                // If DESCRIBE fails, return empty vec - we'll infer from first row
                Ok(Vec::new())
            }
        }
    }

    /// Infers the column count and generates generic column names by attempting to access columns in the first row.
    ///  This is a fallback when DESCRIBE fails.
    fn infer_column_count_and_names(row: &duckdb::Row) -> Result<Vec<String>> {
        let mut column_names = Vec::new();

        // Try to determine column count by attempting to access columns
        // We'll try up to a reasonable maximum (e.g., 100 columns)
        for i in 0..100 {
            // Try different types to see if the column exists
            if row.get::<_, String>(i).is_ok()
                || row.get::<_, i64>(i).is_ok()
                || row.get::<_, u64>(i).is_ok()
                || row.get::<_, f64>(i).is_ok()
                || row.get::<_, bool>(i).is_ok()
                || row.get::<_, Option<String>>(i).is_ok()
            {
                column_names.push(format!("column_{}", i));
            } else {
                break;
            }
        }

        Ok(column_names)
    }

    /// Remove padding of Ethereum addresses to save storage space.
    ///
    /// # Description
    ///
    /// Only the first 24 bytes of the address are removed. The rest are kept regardless of the number of zeros.
    #[inline]
    fn remove_address_padding(addr: &str) -> String {
        // Ensure the address starts with "0x" and it's a full address (66 characters).
        if !addr.starts_with("0x") || addr.len() != 66 {
            return addr.to_string();
        }

        // Check if the input address is aligned to 64B using 0s.
        if &addr[..26] == "0x000000000000000000000000" {
            format!("0x{}", &addr[27..])
        } else {
            addr.to_string()
        }
    }

    /// Converts a `DynSolValue` to a String representation.
    ///
    /// # Description
    ///
    /// For simple types, returns their natural string representation.
    /// For complex types (arrays, tuples), flattens them into a JSON-like string format.
    fn dyn_sol_value_to_string(value: &DynSolValue) -> String {
        match value {
            DynSolValue::Address(a) => Self::remove_address_padding(&a.to_string()),
            DynSolValue::Bool(b) => b.to_string(),
            DynSolValue::Int(i, _) => i.to_string(),
            DynSolValue::Uint(u, _) => u.to_string(),
            DynSolValue::String(s) => s.clone(),
            DynSolValue::FixedBytes(bytes, size) => {
                // Convert fixed bytes to hex string, taking only the relevant bytes
                format!("0x{}", hex::encode(&bytes[..(*size).min(32)]))
            }
            DynSolValue::Bytes(bytes) => {
                // Convert dynamic bytes to hex string
                format!("0x{}", hex::encode(bytes))
            }
            DynSolValue::Function(f) => {
                // Function is 24 bytes: 20 bytes address + 4 bytes selector
                format!("0x{}", hex::encode(f.as_slice()))
            }
            DynSolValue::Array(values) | DynSolValue::FixedArray(values) => {
                // Flatten array into a JSON-like string representation
                let elements: Vec<String> =
                    values.iter().map(Self::dyn_sol_value_to_string).collect();
                format!("[{}]", elements.join(","))
            }
            DynSolValue::Tuple(values) => {
                // Flatten tuple into a JSON-like string representation
                let elements: Vec<String> =
                    values.iter().map(Self::dyn_sol_value_to_string).collect();
                format!("({})", elements.join(","))
            }
        }
    }

    /// Sets the strict mode for the storage.
    pub fn set_strict_mode(&mut self, strict_mode: bool) {
        self.strict_mode = strict_mode;
    }

    /// Gets the strict mode for the storage.
    pub fn strict_mode(&self) -> bool {
        self.strict_mode
    }

    /// Describe the tables in the database.
    ///
    /// # Description
    ///
    /// Returns a JSON array where each element is an object with a single key (the table name)
    /// whose value is an object mapping column names to their types.
    /// The `quixote_info` table is excluded from the results.
    pub fn describe_database(&self) -> Result<Value> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        // Get all tables using SHOW TABLES
        let mut stmt = conn.prepare("SHOW TABLES")?;
        let mut rows = stmt.query([])?;

        let mut tables = Vec::new();
        while let Some(row) = rows.next()? {
            let table_name: String = row.get(0)?;
            // Exclude quixote_info table
            if table_name != DUCKDB_BASE_TABLE_NAME {
                tables.push(table_name);
            }
        }

        // Need to drop the statement and rows before reusing conn
        drop(rows);
        drop(stmt);

        // For each table, get its description
        let mut result = Vec::new();
        for table_name in tables {
            let mut desc_stmt = conn.prepare(&format!("DESCRIBE {}", table_name))?;
            let mut desc_rows = desc_stmt.query([])?;

            let mut columns = Map::new();
            while let Some(row) = desc_rows.next()? {
                // DESCRIBE returns: column_name, column_type, null, key, default, extra
                let column_name: String = row.get(0)?;
                let column_type: String = row.get(1)?;
                columns.insert(column_name, Value::String(column_type));
            }

            let mut table_obj = Map::new();
            table_obj.insert(table_name, Value::Object(columns));
            result.push(Value::Object(table_obj));
        }

        Ok(Value::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::rpc::types::Log;
    use serde_json::json;
    use std::str::FromStr;

    fn transfer_event() -> Event {
        Event::from_str("event Transfer(address indexed from,address indexed to,uint256 amount)")
            .expect("failed to build sample event descriptor")
    }

    fn approval_event() -> Event {
        Event::from_str(
            "event Approval(address indexed owner,address indexed spender,uint256 value)",
        )
        .expect("failed to build approval event descriptor")
    }

    /// Creates 5 transfer logs with some mock data.
    fn get_5_transfer_logs() -> Vec<Log> {
        (0u64..5)
            .map(|i| {
                serde_json::from_value(json!({
                    "address": "0x000000000000000000000000000000000000dead",
                    "topics": [
                        transfer_event().selector().to_string(),
                        "0x000000000000000000000000000000000000000000000000000000000000d00d",
                        "0x000000000000000000000000000000000000000000000000000000000000f00d"
                    ],
                    "data": format!("0x{:064x}", i + 1),
                    "blockNumber": "0x1",
                    "transactionHash": format!("0x{:064x}", i + 1),
                    "transactionIndex": "0x0",
                    "blockHash": "0x000000000000000000000000000000000000000000000000000000000000babe",
                    "logIndex": format!("0x{:x}", i),
                    "removed": false
                }))
                .expect("failed to build transfer log")
            })
            .collect()
    }

    /// Creates 5 approval logs with some mock data.
    fn get_5_approval_logs() -> Vec<Log> {
        (0u64..5)
            .map(|i| {
                serde_json::from_value(json!({
                    "address": "0x000000000000000000000000000000000000d00d",
                    "topics": [
                        approval_event().selector().to_string(),
                        "0x000000000000000000000000000000000000000000000000000000000000d00d",
                        "0x000000000000000000000000000000000000000000000000000000000000f00d"
                    ],
                    "data": format!("0x{:064x}", (i + 1) * 10),
                    "blockNumber": "0x1",
                    "transactionHash": format!("0x{:064x}", i + 100),
                    "transactionIndex": "0x0",
                    "blockHash": "0x0000000000000000000000000000000000000000000000000000000000000002",
                    "logIndex": format!("0x{:x}", i + 10),
                    "removed": false
                }))
                .expect("failed to build approval log")
            })
            .collect()
    }

    /// Given a storage and an event, returns the number of rows stored in the
    /// storage for that event. If the table doesnt exists, it returns 0.
    fn stored_count_for_event(storage: &DuckDBStorage, event: &Event) -> usize {
        // Table name format: event_<chain_id>_<name>_<short_hash>
        let table_name = format!(
            "event_{}_{}_{}",
            TEST_CHAIN_ID,
            event.name.as_str().to_ascii_lowercase(),
            DuckDBStorage::short_hash(&event.selector().to_string())
        );
        let conn = storage
            .conn
            .lock()
            .expect("failed to lock connection for verification");
        match conn.query_row(&format!("SELECT COUNT(*) FROM {table_name}"), [], |row| {
            row.get(0)
        }) {
            Ok(count) => count,
            Err(_) => 0,
        }
    }

    /// Test chain ID used in all tests
    const TEST_CHAIN_ID: u64 = 1;

    #[test]
    fn strict_mode_rejects_unknown_events() {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");

        // Strict mode is enabled. Meaning we require all events to be registered.
        storage.set_strict_mode(true);

        // We register only the transfer event
        storage
            .include_events(TEST_CHAIN_ID, &[transfer_event()])
            .expect("failed to register transfer event");

        // But we try to add both transfer and approval events
        let err = storage
            .add_events(
                TEST_CHAIN_ID,
                &[get_5_approval_logs(), get_5_transfer_logs()].concat(),
            )
            .expect_err("strict mode must error on unknown events");

        // It errors because the approval event is not registered
        assert!(
            err.to_string().contains("event descriptor was not found"),
            "unexpected error: {err}"
        );

        // No transfer events were persisted
        assert_eq!(
            stored_count_for_event(&storage, &transfer_event()),
            0,
            "no transfer events must be stored"
        );

        // No approval events were persisted
        assert_eq!(
            stored_count_for_event(&storage, &approval_event()),
            0,
            "no approval events must be stored"
        );
    }

    #[test]
    fn non_strict_mode_skips_unknown_events() {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");

        // Strict mode is disabled. Meaning we just skip unknown events without failing.
        storage.set_strict_mode(false);

        // Register only the Transfer event
        storage
            .include_events(TEST_CHAIN_ID, &[transfer_event()])
            .expect("failed to register event");

        // We try to add both transfer and approval events
        storage
            .add_events(
                TEST_CHAIN_ID,
                &[get_5_approval_logs(), get_5_transfer_logs()].concat(),
            )
            .expect("non-strict mode should skip unknown events without failing");

        // Transfer events were persisted (verified by querying the DB)
        assert_eq!(
            stored_count_for_event(&storage, &transfer_event()),
            5,
            "transfer events must be stored"
        );

        // Approval events were skipped
        assert_eq!(
            stored_count_for_event(&storage, &approval_event()),
            0,
            "approval events must be skipped"
        );
    }

    #[test]
    fn strict_mode_works_with_known_events() {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");

        // Set strict mode. We require all events to be registered.
        storage.set_strict_mode(true);

        // Register both events
        storage
            .include_events(TEST_CHAIN_ID, &[transfer_event(), approval_event()])
            .expect("failed to register events");

        // Insert both events
        storage
            .add_events(
                TEST_CHAIN_ID,
                &[get_5_approval_logs(), get_5_transfer_logs()].concat(),
            )
            .expect("strict mode should accept known events");

        // Transfer events are persisted
        assert_eq!(
            stored_count_for_event(&storage, &transfer_event()),
            5,
            "all transfer logs must be stored"
        );

        // Approval events are persisted
        assert_eq!(
            stored_count_for_event(&storage, &approval_event()),
            5,
            "all approval events must be stored"
        );
    }

    fn erc721_transfer_event() -> Event {
        Event::from_str(
            "event Transfer(address indexed from,address indexed to,uint256 indexed tokenId)",
        )
        .expect("failed to build ERC721 Transfer event")
    }

    fn erc20_transfer_event() -> Event {
        Event::from_str("event Transfer(address indexed from,address indexed to,uint256 amount)")
            .expect("failed to build ERC20 Transfer event")
    }

    fn event_table_name(event: &Event) -> String {
        format!(
            "event_{}_{}_{}",
            TEST_CHAIN_ID,
            event.name.as_str().to_ascii_lowercase(),
            DuckDBStorage::short_hash(&event.selector().to_string())
        )
    }

    fn storage_with(events: &[Event]) -> DuckDBStorage {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");
        storage.set_strict_mode(true);
        storage
            .include_events(TEST_CHAIN_ID, events)
            .expect("failed to register events");
        storage
    }

    #[test]
    fn uint256_values_are_stored_as_decimal() {
        let erc721 = erc721_transfer_event();
        let storage = storage_with(&[erc721.clone()]);

        // Token id in hex format (0xbeef = 48879 decimal)
        let token_id_hex = "0x000000000000000000000000000000000000000000000000000000000000beef";
        let token_id_decimal = "48879";

        let log: Log = serde_json::from_value(json!({
            "address": "0x000000000000000000000000000000000000c0de",
            "topics": [
                erc721.selector().to_string(),
                "0x000000000000000000000000000000000000000000000000000000000000d00d",
                "0x000000000000000000000000000000000000000000000000000000000000f00d",
                // Token id goes here
                token_id_hex,
            ],
            "data": "0x",
            "blockNumber": "0x1",
            "transactionHash": format!("0x{:064x}", 0xaaa_u64),
            "transactionIndex": "0x0",
            "blockHash": format!("0x{:064x}", 0xbbb_u64),
            "logIndex": "0x0",
            "removed": false
        }))
        .expect("failed to build erc721 log");

        storage
            .add_events(TEST_CHAIN_ID, &[log])
            .expect("erc721 transfer should be accepted");

        let table = event_table_name(&erc721);
        let conn = storage
            .conn
            .lock()
            .expect("failed to lock connection for verification");
        let stored_token_id: String = conn
            .query_row(
                &format!("SELECT CAST(tokenId AS VARCHAR) FROM {table}"),
                [],
                |row| row.get(0),
            )
            .expect("tokenId must exist");

        // uint256 values are stored as BIGNUM and retrieved as decimal strings
        assert_eq!(
            stored_token_id, token_id_decimal,
            "tokenId should be stored as decimal"
        );
    }

    #[test]
    fn addresses_are_stripped_correctly() {
        let erc20 = erc20_transfer_event();
        let storage = storage_with(&[erc20.clone()]);

        // This is the address we want to store
        let from_address = "0x000083970c0bd792a6d1402a12c65628bcb3f8b4";

        let log: Log = serde_json::from_value(json!({
            "address": "0x000000000000000000000000000000000000c0de",
            "topics": [
                erc20.selector().to_string(),
                // Addresses are 40 bytes but they are used as 64 bytes
                format!("{:0>64}", &from_address.trim_start_matches("0x")),
                "0x000000000000000000000000000000000000000000000000000000000000f00d",
            ],
            "data": format!("0x{:064x}", 0u64),
            "blockNumber": "0x2",
            "transactionHash": format!("0x{:064x}", 0xccc_u64),
            "transactionIndex": "0x0",
            "blockHash": format!("0x{:064x}", 0xddd_u64),
            "logIndex": "0x0",
            "removed": false
        }))
        .expect("failed to build erc20 log");

        storage
            .add_events(TEST_CHAIN_ID, &[log])
            .expect("erc20 transfer should be accepted");

        let table = event_table_name(&erc20);
        let conn = storage
            .conn
            .lock()
            .expect("failed to lock connection for verification");
        let stored_from: String = conn
            .query_row(&format!("SELECT \"from\" FROM {table}"), [], |row| {
                row.get(0)
            })
            .expect("from must exist");

        // We retrieve the expected address
        assert_eq!(
            stored_from.to_ascii_lowercase(),
            from_address.to_ascii_lowercase()
        );
    }

    /// Represents a test case for verifying Solidity event type storage.
    struct SolidityTypeTestCase {
        /// The Solidity event signature (e.g., "event Test(uint256 value)")
        event_signature: &'static str,
        /// The input value to encode and store
        input_value: DynSolValue,
        /// The expected column name in DuckDB
        expected_name: &'static str,
        /// The expected DuckDB column type (e.g., "VARINT", "VARCHAR")
        expected_type: &'static str,
        /// The expected value as a string (use CAST to VARCHAR for comparison)
        expected_value: &'static str,
    }

    /// Test that all Solidity data types are properly stored in the database.
    #[test]
    fn all_solidity_types_stored_correctly() {
        use alloy::primitives::{Address, FixedBytes, I256, U256};

        fn to_bytes32_hex(val: u64) -> String {
            format!("0x{:064x}", val)
        }

        let test_cases: Vec<SolidityTypeTestCase> = vec![
            // address (indexed)
            SolidityTypeTestCase {
                event_signature: "event Test(address indexed addr)",
                input_value: DynSolValue::Address(
                    "0xabcdef1234567890abcdef1234567890abcdef12"
                        .parse::<Address>()
                        .unwrap(),
                ),
                expected_name: "addr",
                expected_type: "VARCHAR",
                expected_value: "0xabcdef1234567890abcdef1234567890abcdef12",
            },
            // address (non-indexed)
            SolidityTypeTestCase {
                event_signature: "event Test(address addr)",
                input_value: DynSolValue::Address(
                    "0xabcdef1234567890abcdef1234567890abcdef12"
                        .parse::<Address>()
                        .unwrap(),
                ),
                expected_name: "addr",
                expected_type: "VARCHAR",
                expected_value: "0xabcdef1234567890abcdef1234567890abcdef12",
            },
            // bool true
            SolidityTypeTestCase {
                event_signature: "event Test(bool flag)",
                input_value: DynSolValue::Bool(true),
                expected_name: "flag",
                expected_type: "VARCHAR",
                expected_value: "true",
            },
            // bool false
            SolidityTypeTestCase {
                event_signature: "event Test(bool flag)",
                input_value: DynSolValue::Bool(false),
                expected_name: "flag",
                expected_type: "VARCHAR",
                expected_value: "false",
            },
            // uint256
            SolidityTypeTestCase {
                event_signature: "event Test(uint256 value)",
                input_value: DynSolValue::Uint(U256::from(12345u64), 256),
                expected_name: "value",
                expected_type: "BIGNUM",
                expected_value: "12345",
            },
            // uint128
            SolidityTypeTestCase {
                event_signature: "event Test(uint128 value)",
                input_value: DynSolValue::Uint(U256::from(255u64), 128),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "255",
            },
            // uint64
            SolidityTypeTestCase {
                event_signature: "event Test(uint64 value)",
                input_value: DynSolValue::Uint(U256::from(0xdeadbeefu64), 64),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "3735928559",
            },
            // uint32
            SolidityTypeTestCase {
                event_signature: "event Test(uint32 value)",
                input_value: DynSolValue::Uint(U256::from(1000000u64), 32),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "1000000",
            },
            // uint16
            SolidityTypeTestCase {
                event_signature: "event Test(uint16 value)",
                input_value: DynSolValue::Uint(U256::from(65535u64), 16),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "65535",
            },
            // uint8
            SolidityTypeTestCase {
                event_signature: "event Test(uint8 value)",
                input_value: DynSolValue::Uint(U256::from(255u64), 8),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "255",
            },
            // int256 negative
            SolidityTypeTestCase {
                event_signature: "event Test(int256 value)",
                input_value: DynSolValue::Int(I256::MINUS_ONE, 256),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-1",
            },
            // int256 positive
            SolidityTypeTestCase {
                event_signature: "event Test(int256 value)",
                input_value: DynSolValue::Int(I256::try_from(123i64).unwrap(), 256),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "123",
            },
            // int128
            SolidityTypeTestCase {
                event_signature: "event Test(int128 value)",
                input_value: DynSolValue::Int(I256::try_from(-123i64).unwrap(), 128),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-123",
            },
            // int64
            SolidityTypeTestCase {
                event_signature: "event Test(int64 value)",
                input_value: DynSolValue::Int(I256::try_from(-2i64).unwrap(), 64),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-2",
            },
            // int32
            SolidityTypeTestCase {
                event_signature: "event Test(int32 value)",
                input_value: DynSolValue::Int(I256::try_from(-42i64).unwrap(), 32),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-42",
            },
            // int16
            SolidityTypeTestCase {
                event_signature: "event Test(int16 value)",
                input_value: DynSolValue::Int(I256::try_from(-100i64).unwrap(), 16),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-100",
            },
            // int8
            SolidityTypeTestCase {
                event_signature: "event Test(int8 value)",
                input_value: DynSolValue::Int(I256::MINUS_ONE, 8),
                expected_name: "value",
                expected_type: "VARCHAR",
                expected_value: "-1",
            },
            // bytes32
            SolidityTypeTestCase {
                event_signature: "event Test(bytes32 data)",
                input_value: DynSolValue::FixedBytes(
                    FixedBytes::<32>::from_slice(
                        &hex::decode(
                            "deadbeef00000000000000000000000000000000000000000000000000000001",
                        )
                        .unwrap(),
                    ),
                    32,
                ),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xdeadbeef00000000000000000000000000000000000000000000000000000001",
            },
            // bytes16
            SolidityTypeTestCase {
                event_signature: "event Test(bytes16 data)",
                input_value: DynSolValue::FixedBytes(
                    FixedBytes::<32>::right_padding_from(
                        &hex::decode("deadbeefcafebabe1234567800000000").unwrap(),
                    ),
                    16,
                ),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xdeadbeefcafebabe1234567800000000",
            },
            // bytes8
            SolidityTypeTestCase {
                event_signature: "event Test(bytes8 data)",
                input_value: DynSolValue::FixedBytes(
                    FixedBytes::<32>::right_padding_from(&hex::decode("deadbeefcafebabe").unwrap()),
                    8,
                ),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xdeadbeefcafebabe",
            },
            // bytes4
            SolidityTypeTestCase {
                event_signature: "event Test(bytes4 data)",
                input_value: DynSolValue::FixedBytes(
                    FixedBytes::<32>::right_padding_from(&hex::decode("deadbeef").unwrap()),
                    4,
                ),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xdeadbeef",
            },
            // bytes1
            SolidityTypeTestCase {
                event_signature: "event Test(bytes1 data)",
                input_value: DynSolValue::FixedBytes(
                    FixedBytes::<32>::right_padding_from(&[0xab]),
                    1,
                ),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xab",
            },
            // bytes (dynamic)
            SolidityTypeTestCase {
                event_signature: "event Test(bytes data)",
                input_value: DynSolValue::Bytes(hex::decode("deadbeef").unwrap()),
                expected_name: "data",
                expected_type: "VARCHAR",
                expected_value: "0xdeadbeef",
            },
            // string
            SolidityTypeTestCase {
                event_signature: "event Test(string message)",
                input_value: DynSolValue::String("hello".to_string()),
                expected_name: "message",
                expected_type: "VARCHAR",
                expected_value: "hello",
            },
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let event = Event::from_str(test_case.event_signature).unwrap_or_else(|_| {
                panic!(
                    "Failed to parse event signature: {}",
                    test_case.event_signature
                )
            });
            let storage = storage_with(&[event.clone()]);

            // Each test event has exactly one parameter
            let param = &event.inputs[0];

            // Encode based on whether the param is indexed (topic) or not (data)
            let (topics, data) = if param.indexed {
                let encoded = test_case.input_value.abi_encode();
                (
                    vec![
                        event.selector().to_string(),
                        format!("0x{}", hex::encode(&encoded)),
                    ],
                    "0x".to_string(),
                )
            } else {
                let encoded = test_case.input_value.abi_encode_params();
                (
                    vec![event.selector().to_string()],
                    format!("0x{}", hex::encode(&encoded)),
                )
            };

            let log: Log = serde_json::from_value(json!({
                "address": "0x000000000000000000000000000000000000c0de",
                "topics": topics,
                "data": data,
                "blockNumber": format!("0x{:x}", i + 1),
                "transactionHash": to_bytes32_hex((i + 1) as u64),
                "transactionIndex": "0x0",
                "blockHash": to_bytes32_hex((i + 100) as u64),
                "logIndex": "0x0",
                "removed": false
            }))
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to build log for {}: {}",
                    test_case.event_signature, e
                )
            });

            // Add log to storage
            storage
                .add_events(TEST_CHAIN_ID, &[log])
                .unwrap_or_else(|e| {
                    panic!("Failed to store event {}: {}", test_case.event_signature, e)
                });

            let table = event_table_name(&event);
            let conn = storage.conn.lock().expect("Failed to lock connection");

            // 1. Verify column name exists
            let col_info: (String, String) = conn
                .query_row(
                    &format!(
                        "SELECT column_name, data_type FROM information_schema.columns \
                         WHERE table_name = '{}' AND column_name = '{}'",
                        table, test_case.expected_name
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "Column '{}' must exist for event '{}': {}",
                        test_case.expected_name, test_case.event_signature, e
                    )
                });

            // 2. Verify column type
            assert_eq!(
                col_info.1.to_uppercase(),
                test_case.expected_type,
                "Type mismatch for '{}': expected '{}', got '{}'",
                test_case.event_signature,
                test_case.expected_type,
                col_info.1
            );

            // 3. Verify stored value (cast to VARCHAR for uniform comparison)
            let stored_value: String = conn
                .query_row(
                    &format!(
                        "SELECT CAST({} AS VARCHAR) FROM {}",
                        test_case.expected_name, table
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to read value for '{}': {}",
                        test_case.event_signature, e
                    )
                });

            assert_eq!(
                stored_value.to_lowercase(),
                test_case.expected_value.to_lowercase(),
                "Value mismatch for '{}': expected '{}', got '{}'",
                test_case.event_signature,
                test_case.expected_value,
                stored_value
            );
        }
    }
}

// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

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
use duckdb::{Connection, OptionalExt, params};
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
    fn add_events(&self, events: &[Log]) -> Result<()> {
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

        // First stage: insert the new blocks into the blocks table.
        {
            // Ensure unique blocks are added to the blocks table.
            let mut blocks_appender = tx.appender("blocks")?;
            let mut last_block_number = 0;
            for event in events {
                if let Some(block_number) = event.block_number
                    && block_number != last_block_number
                {
                    blocks_appender.append_row(params![
                        block_number.to_string(),
                        event.block_hash.unwrap().to_string(),
                        event.block_timestamp.unwrap_or_default().to_string()
                    ])?;
                    last_block_number = block_number;
                } else {
                    continue;
                }
            }

            blocks_appender.flush()?;
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

            // Parse the received events (as hashes) into event descriptors using the local cache of known events.
            // Only known events by the indexer can be processed. Thus if the indexer is running in strict mode, the
            // indexing will stop if the event descriptor is not found.
            let event = match self
                .event_descriptors
                .read()
                .map_err(|_| {
                    anyhow::anyhow!("Failed to acquire read lock for the event descriptor")
                })?
                .get(&event_hash)
            {
                Some(event) => event.clone(),
                None => {
                    if self.strict_mode {
                        return Err(anyhow::anyhow!(
                            "The event {event_hash} was received but the event descriptor was not found. You can either include the event in the command line arguments or run the indexer with the strict mode disabled."
                        ));
                    } else {
                        warn!(
                            "The event {event_hash} was received but the event descriptor was not found. This event will be ignored."
                        );
                        continue;
                    }
                }
            };

            let table_name = format!(
                "event_{}_{event_hash}",
                event.name.as_str().to_ascii_lowercase()
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

            // Get event_hash from table name (remove "event_" prefix)
            let event_hash = table_name.split("_").nth(2).unwrap().to_string();

            // Get the parsed event from the event descriptors
            let parsed_event = self
                .event_descriptors
                .read()
                .map_err(|_| anyhow::anyhow!("Failed to acquire read lock for the event descriptor"))?
                .get(&event_hash)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Event descriptor not found for hash: {event_hash}. Ensure the event is registered before processing.",
                    )
                })?
                .clone();

            let mut appender = tx.appender(&table_name)?;

            for log in table_events {
                // Insert the always present fields
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
                    "UPDATE event_descriptor SET last_block = ?",
                    [log.block_number.unwrap().to_string()],
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
            "SELECT event_hash, event_signature, event_name, first_block, last_block FROM event_descriptor",
        )?;
        let mut events = rows
            .query_map([], |row| {
                Ok(EventDescriptorDb {
                    event_hash: row.get(0).optional()?,
                    event_signature: row.get(1).optional()?,
                    event_name: row.get(2).optional()?,
                    first_block: row.get(3).optional()?,
                    last_block: row.get(4).optional()?,
                    event_count: None,
                })
            })?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<EventDescriptorDb>>>()?;

        // Now, let's populate the event count
        for event in events.iter_mut() {
            let table_name = format!(
                "event_{}_{}",
                event.event_name.as_ref().unwrap().to_ascii_lowercase(),
                event.event_hash.as_ref().unwrap()
            );

            let event_count: usize =
                conn.query_row(&format!("SELECT COUNT(*) FROM {table_name}",), [], |row| {
                    row.get(0)
                })?;
            event.event_count = Some(event_count);
        }

        Ok(events)
    }

    #[inline]
    fn last_block(&self, event: &Event) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            "SELECT last_block FROM event_descriptor WHERE event_hash = ?",
            [event.selector().to_string()],
            |row| row.get(0),
        )?)
    }

    #[inline]
    fn first_block(&self, event: &Event) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            "SELECT first_block FROM event_descriptor WHERE event_hash = ?",
            [event.selector().to_string()],
            |row| row.get(0),
        )?)
    }

    fn include_events(&self, events: &[Event]) -> Result<()> {
        DuckDBStorage::create_event_schema(&self.conn, events)?;
        let mut event_descriptors = self.event_descriptors.write().map_err(|_| {
            anyhow::anyhow!("Failed to acquire write lock for the event descriptor")
        })?;
        // Populate the local cache of the indexed events.
        for event in events {
            event_descriptors.insert(event.selector().to_string(), event.clone());
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

    fn event_index_status(&self, event: &Event) -> Result<Option<EventStatus>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        let Some((mut event_status, event_name)) = conn
            .query_row(
                "SELECT \"event_name\", \"first_block\", \"last_block\" FROM event_descriptor WHERE event_hash = ?",
                [event.selector().to_string()],
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
            // Get the event count
            let event_count: usize = conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM event_{}_{}",
                    event_name.to_ascii_lowercase(),
                    event.selector()
                ),
                [],
                |row| row.get(0),
            )?;

            event_status.event_count = event_count;
        }

        Ok(Some(event_status))
    }

    // TODO: what if we run multiple -e tasks?
    fn synchronize_events(&self, last_processed: Option<u64>) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;

        if let Some(last_processed) = last_processed {
            conn.execute(
                "UPDATE event_descriptor SET last_block = ?",
                [last_processed.to_string()],
            )?;
        } else {
            conn.execute(
                "UPDATE event_descriptor SET last_block = (SELECT MAX(last_block) FROM event_descriptor)",
                [],
            )?;
        }

        debug!("Events synchronized to the latest block");

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
        let mut statement = conn.prepare("SELECT event_hash, event_name FROM event_descriptor")?;

        let event_descriptors = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .map(|r| r.unwrap())
            .collect::<Vec<(String, String)>>();

        let mut contracts = HashSet::new();
        for (event_hash, event_name) in event_descriptors {
            let table_name = format!("event_{}_{}", event_name.to_ascii_lowercase(), event_hash);

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
        let statement = format!(
            "
            BEGIN;
            CREATE TABLE IF NOT EXISTS {DUCKDB_BASE_TABLE_NAME}(
                version VARCHAR NOT NULL,
                PRIMARY KEY (version)
            );
            CREATE TABLE IF NOT EXISTS blocks(
                block_number UBIGINT NOT NULL,
                block_hash VARCHAR(66) NOT NULL,
                block_timestamp UBIGINT NOT NULL,
                PRIMARY KEY (block_number)
            );
            CREATE TABLE IF NOT EXISTS event_descriptor(
                event_hash VARCHAR(66) NOT NULL,
                event_signature VARCHAR(256) NOT NULL,
                event_name VARCHAR(40) NOT NULL,
                first_block UBIGINT,
                last_block UBIGINT,
                PRIMARY KEY (event_hash)
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

    fn create_event_schema(conn: &Mutex<Connection>, events: &[Event]) -> Result<()> {
        for event in events {
            // These will be the name used to create the new table: event_<name>_<hash>.
            let table_name = B256::from(event.selector()).to_string();
            let event_name = event.name.as_str().to_ascii_lowercase();

            debug!("Creating event schema for: {}", event.full_signature());

            // Now build the table definition
            let mut statement = format!(
                "
                    CREATE TABLE IF NOT EXISTS event_{event_name}_{table_name}(
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

            // Now, create an entry for such event in the event_descriptor table.
            statement.push_str(&format!("INSERT INTO event_descriptor (event_hash, event_signature, event_name, first_block, last_block) VALUES ('{}', '{}', '{}', 0, 0) ON CONFLICT (event_hash) DO NOTHING;", event.selector(), event.full_signature(), event.name));

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
    pub fn set_first_block(&self, event: &Event, block_number: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        conn.execute(
            "UPDATE event_descriptor SET first_block = ? WHERE event_hash = ?",
            [block_number.to_string(), event.selector().to_string()],
        )?;
        Ok(())
    }

    /// Attempts to infer the JSON value type for a column in a row.
    ///
    /// # Description
    ///
    /// Tries conversions in order: Number (i64, u64, f64), Bool, String, Null.
    fn infer_value_type(row: &duckdb::Row, col_idx: usize) -> Result<Value> {
        // Try Option<String>
        if let Ok(opt_str) = row.get::<_, Option<String>>(col_idx) {
            return match opt_str {
                Some(s) => Ok(Value::String(s)),
                None => Ok(Value::Null),
            };
        }

        // Try Option<i64>
        if let Ok(opt_val) = row.get::<_, Option<i64>>(col_idx) {
            return match opt_val {
                Some(val) => Ok(Value::Number(Number::from(val))),
                None => Ok(Value::Null),
            };
        }

        // Try Option<u64>
        if let Ok(opt_val) = row.get::<_, Option<u64>>(col_idx) {
            return match opt_val {
                Some(val) => {
                    // serde_json::Number doesn't support u64 directly, so we need to convert
                    if let Some(num) = Number::from_f64(val as f64) {
                        Ok(Value::Number(num))
                    } else {
                        // If conversion fails, use string representation
                        Ok(Value::String(val.to_string()))
                    }
                }
                None => Ok(Value::Null),
            };
        }

        // Try Option<f64>
        if let Ok(opt_val) = row.get::<_, Option<f64>>(col_idx) {
            return match opt_val {
                Some(val) => {
                    if let Some(num) = Number::from_f64(val) {
                        Ok(Value::Number(num))
                    } else {
                        // NaN or Infinity - represent as string
                        Ok(Value::String(val.to_string()))
                    }
                }
                None => Ok(Value::Null),
            };
        }

        // Try Option<bool>
        if let Ok(opt_val) = row.get::<_, Option<bool>>(col_idx) {
            return match opt_val {
                Some(val) => Ok(Value::Bool(val)),
                None => Ok(Value::Null),
            };
        }

        // Try non-optional types
        // Try i64 (signed integer)
        if let Ok(val) = row.get::<_, i64>(col_idx) {
            return Ok(Value::Number(Number::from(val)));
        }

        // Try u64 (unsigned integer)
        if let Ok(val) = row.get::<_, u64>(col_idx) {
            // serde_json::Number doesn't support u64 directly, so we need to convert
            if let Some(num) = Number::from_f64(val as f64) {
                return Ok(Value::Number(num));
            }
            // If conversion fails, fall through to string
        }

        // Try f64 (floating point)
        if let Ok(val) = row.get::<_, f64>(col_idx)
            && let Some(num) = Number::from_f64(val)
        {
            return Ok(Value::Number(num));
            // If conversion fails (NaN, Infinity), fall through to string
        }

        // Try bool
        if let Ok(val) = row.get::<_, bool>(col_idx) {
            return Ok(Value::Bool(val));
        }

        // Try String (this should work for most remaining types)
        if let Ok(val) = row.get::<_, String>(col_idx) {
            return Ok(Value::String(val));
        }

        // If all else fails, return null
        Ok(Value::Null)
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
        // Note: This logic is duplicated. Table name should be given by the storage.
        let table_name = format!(
            "event_{}_{}",
            event.name.as_str().to_ascii_lowercase(),
            event.selector()
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

    #[test]
    fn strict_mode_rejects_unknown_events() {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");

        // Strict mode is enabled. Meaning we require all events to be registered.
        storage.set_strict_mode(true);

        // We register only the transfer event
        storage
            .include_events(&[transfer_event()])
            .expect("failed to register transfer event");

        // But we try to add both transfer and approval events
        let err = storage
            .add_events(&[get_5_approval_logs(), get_5_transfer_logs()].concat())
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
            .include_events(&[transfer_event()])
            .expect("failed to register event");

        // We try to add both transfer and approval events
        storage
            .add_events(&[get_5_approval_logs(), get_5_transfer_logs()].concat())
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
            .include_events(&[transfer_event(), approval_event()])
            .expect("failed to register events");

        // Insert both events
        storage
            .add_events(&[get_5_approval_logs(), get_5_transfer_logs()].concat())
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
            "event_{}_{}",
            event.name.as_str().to_ascii_lowercase(),
            event.selector()
        )
    }

    fn storage_with(events: &[Event]) -> DuckDBStorage {
        let mut storage = DuckDBStorage::with_db(":memory:").expect("in-memory DB should open");
        storage.set_strict_mode(true);
        storage
            .include_events(events)
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
            .add_events(&[log])
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
            .add_events(&[log])
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
                expected_type: "VARINT",
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
            storage.add_events(&[log]).unwrap_or_else(|e| {
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

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
use std::{collections::HashMap, string::ToString, sync::Mutex};
use tracing::{debug, error, info, warn};

/// Implementation of the Storage trait for the DuckDB database.
pub struct DuckDBStorage {
    conn: Mutex<Connection>,
    db_path: String,
    event_descriptors: Mutex<HashMap<String, String>>,
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
    fn add_events(&self, events: &[Log]) -> Result<usize> {
        // Quick check to avoid unnecessary operations.
        if events.is_empty() {
            info!("No events for the given block range");
            return Ok(usize::default());
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

        // Cache parsed events by event_hash to avoid repeated DB queries and parsing
        let mut event_cache: HashMap<String, Event> = HashMap::new();

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

            let event_signature = self
                .event_descriptors
                .lock()
                .map_err(|_| anyhow::anyhow!("Failed to acquire lock for the event descriptor"))?
                .get(&event_hash)
                .unwrap()
                .clone();

            let event_name = Event::parse(&event_signature)
                .unwrap()
                .name
                .as_str()
                .to_ascii_lowercase();
            let table_name = format!("event_{event_name}_{event_hash}");
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

            // Get or cache the parsed event
            let parsed_event = event_cache.entry(event_hash.clone()).or_insert_with(|| {
                let event_signature: String = tx
                    .query_row(
                        "SELECT event_signature FROM event_descriptor WHERE event_hash = ?",
                        [event_hash.as_str()],
                        |row| row.get(0),
                    )
                    .expect("Event signature not found in database");
                Event::parse(&event_signature).expect("Failed to parse event signature")
            });

            let mut appender = tx.appender(&table_name)?;

            for log in table_events {
                // Insert the always present fields
                let mut row_vals: Vec<String> = vec![
                    log.block_number.unwrap().to_string(),
                    log.transaction_hash.unwrap().to_string(),
                    log.log_index.unwrap().to_string(),
                    log.address().to_string(),
                ];

                // Now we need to consider how many indexed topics our current event includes.
                let indexed_params = parsed_event
                    .inputs
                    .iter()
                    .filter(|input| input.indexed)
                    .count();
                for i in 1..=indexed_params {
                    row_vals.push(
                        log.topics()
                            .get(i)
                            .map(|t| t.to_string())
                            .unwrap_or_default(),
                    );
                }

                // Time to include the non-indexed parameters.
                if let Ok(DecodedEvent { body, .. }) =
                    parsed_event.decode_log_parts(log.topics().to_vec(), log.data().data.as_ref())
                {
                    for item in body {
                        let value = match item {
                            DynSolValue::Address(a) => a.to_string(),
                            DynSolValue::Bool(b) => b.to_string(),
                            DynSolValue::Int(i, _) => i.to_string(),
                            DynSolValue::Uint(u, _) => u.to_string(),
                            DynSolValue::String(s) => s.to_string(),
                            _ => {
                                error!("Unsupported value: {:?}", item);
                                continue;
                            }
                        };
                        row_vals.push(value);
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

        Ok(events.len())
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
        let mut event_descriptors = self
            .event_descriptors
            .lock()
            .map_err(|_| anyhow::anyhow!("Failed to acquire lock for the event descriptor"))?;
        for event in events {
            event_descriptors.insert(
                event.selector().to_string(),
                event.full_signature().to_string(),
            );
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
        let storage = self.clone();
        if !query.trim_start().to_uppercase().starts_with("SELECT") {
            return Ok(json!({ "error": "Query must be a SELECT statement" }));
        }

        let conn = storage
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
        todo!()
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
            // Try to retrieve version from etherduck_info table using a query
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
            event_descriptors: Mutex::new(HashMap::new()),
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
                        transaction_hash VARCHAR(42) NOT NULL,
                        log_index USMALLINT NOT NULL,
                        contract_address VARCHAR(42) NOT NULL,
                ",
            );

            event.inputs.iter().for_each(|param| {
                statement.push_str(&format!(
                    "\"{}\" VARCHAR({DEFAULT_VARCHAR_LENGTH}),",
                    param.name
                ));
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
}

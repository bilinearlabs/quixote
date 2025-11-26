// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the connection to the DuckDB database.

use crate::{
    constants::*,
    error_codes::ERROR_CODE_DATABASE_LOCKED,
    storage::{ContractDescriptorDb, EventDb, EventDescriptorDb, Storage, StorageQuery},
};
use alloy::{
    dyn_abi::{DecodedEvent, DynSolValue, EventExt},
    json_abi::Event,
    primitives::{Address, B256},
    rpc::types::Log,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::{Connection, params};
use serde_json::{Map, Number, Value, json};
use std::{collections::HashMap, string::ToString, sync::Mutex};
use tracing::{debug, error, info, warn};

/// Implementation of the Storage trait for the DuckDB database.
pub struct DuckDBStorage {
    conn: Mutex<Connection>,
    table_regex: regex::Regex,
    db_path: String,
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
            let event_hash = log.topic0().unwrap().to_string();
            let table_name = format!("event_{event_hash}");
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
            let event_hash = table_name.strip_prefix("event_").unwrap().to_string();

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
                // Build row values
                let mut row_vals: Vec<String> = vec![
                    log.block_number.unwrap().to_string(),
                    log.transaction_hash.unwrap().to_string(),
                    log.log_index.unwrap().to_string(),
                    log.address().to_string(),
                    event_hash.clone(),
                    log.topics()
                        .get(1)
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                    log.topics()
                        .get(2)
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                    log.topics()
                        .get(3)
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                ];

                // Decode and append event parameters
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
            }

            // Flush the appender for this table
            appender.flush()?;
        }

        // Update the last block within the same transaction
        if let Some(last_event) = events.last()
            && let Some(last_block_number) = last_event.block_number
        {
            tx.execute(
                &format!("UPDATE {DUCKDB_BASE_TABLE_NAME} SET last_block = ?"),
                [last_block_number.to_string()],
            )?;
        }

        // Explicitly commit the transaction
        tx.commit()?;

        Ok(())
    }

    fn list_indexed_events(&self) -> Result<Vec<Event>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut rows = conn.prepare("SELECT * FROM event_descriptor")?;
        let events = rows
            .query_map([], |row| {
                Ok(EventDescriptorDb {
                    event_hash: row.get(0).unwrap(),
                    event_signature: row.get(1).unwrap(),
                    event_name: row.get(2).unwrap(),
                })
            })?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<EventDescriptorDb>>>()?
            .iter()
            .map(|e| Event::parse(&e.event_signature).unwrap())
            .collect::<Vec<Event>>();

        Ok(events)
    }

    #[inline]
    fn last_block(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            &format!("SELECT last_block FROM {}", DUCKDB_BASE_TABLE_NAME),
            [],
            |row| row.get(0),
        )?)
    }

    #[inline]
    fn first_block(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        Ok(conn.query_row(
            &format!("SELECT first_block FROM {}", DUCKDB_BASE_TABLE_NAME),
            [],
            |row| row.get(0),
        )?)
    }

    fn include_events(&self, events: &[Event]) -> Result<()> {
        debug!("Including events: {events:?} in the database");
        let registered_events = self.list_indexed_events()?;
        let not_registered_events: Vec<&Event> = events
            .iter()
            .filter(|e| !registered_events.contains(e))
            .collect();
        DuckDBStorage::create_event_schema(&self.conn, &not_registered_events)
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
            let version: String =
                conn.query_row("SELECT version FROM etherduck_info LIMIT 1", [], |row| {
                    row.get(0)
                })?;

            if version != DUCKDB_SCHEMA_VERSION {
                warn!("Your database is out of date. Please run the database upgrade.");
            }
            Mutex::new(conn)
        };

        // This regex will match the table names after FROM and any JOINs (supports INNER, LEFT, RIGHT, FULL, CROSS).
        // It captures each table name in group 1, ignoring keywords.
        let table_regex =
            regex::Regex::new(r"(?i)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();

        Ok(DuckDBStorage {
            conn: conn_mutex,
            table_regex,
            db_path: db_path.to_string(),
        })
    }

    fn create_db_base(conn: &Mutex<Connection>) -> Result<()> {
        let statement = format!(
            "
            BEGIN;
            CREATE TABLE IF NOT EXISTS {DUCKDB_BASE_TABLE_NAME}(
                version VARCHAR NOT NULL,
                first_block UBIGINT,
                last_block UBIGINT,
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
                    "INSERT INTO {} (version, first_block, last_block)
                VALUES (?, 0, 0);",
                    DUCKDB_BASE_TABLE_NAME
                ),
                [DUCKDB_SCHEMA_VERSION],
            )?;
        }

        Ok(())
    }

    fn create_event_schema(conn: &Mutex<Connection>, events: &[&Event]) -> Result<()> {
        for event in events {
            // These will be the name used to create the new table: event_<hash>.
            let table_name = B256::from(event.selector()).to_string();
            let not_indexed_params_length = event.inputs.iter().filter(|p| !p.indexed).count();
            let indexed_params_length = event.inputs.iter().filter(|p| p.indexed).count();

            // Now build the table definition
            let mut statement = format!(
                "
                    CREATE TABLE IF NOT EXISTS event_{table_name}(
                        block_number UBIGINT NOT NULL,
                        transaction_hash VARCHAR(42) NOT NULL,
                        log_index USMALLINT NOT NULL,
                        contract_address VARCHAR(42) NOT NULL,
                ",
            );

            // There are events with no parameters at all.
            if indexed_params_length > 0 {
                statement.push_str(
                    "                        
                        topic0 VARCHAR(66),
                        topic1 VARCHAR(66),
                        topic2 VARCHAR(66),
                        topic3 VARCHAR(66),",
                );
            }

            (0..not_indexed_params_length)
                .for_each(|i| statement.push_str(&format!("input_{i} VARCHAR(66),")));
            statement.push_str("PRIMARY KEY (block_number, transaction_hash, log_index));");

            // Now, create an entry for such event in the event_descriptor table.
            statement.push_str(&format!("INSERT INTO event_descriptor (event_hash, event_signature, event_name) VALUES ('{}', '{}', '{}') ON CONFLICT (event_hash) DO NOTHING;", event.selector(), event.full_signature(), event.name));

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
    pub fn set_first_block(&self, block_number: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        conn.execute(
            &format!("UPDATE {DUCKDB_BASE_TABLE_NAME} SET first_block = ?"),
            [block_number.to_string()],
        )?;
        Ok(())
    }

    fn get_db_tables(&self) -> Result<Vec<String>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut rows = conn.prepare("SHOW tables")?;
        let tables = rows
            .query_map([], |row| Ok(row.get(0).unwrap()))?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<String>>>()?;
        Ok(tables)
    }

    // Returns pairs (Column Name, Column Type)
    fn get_table_schema(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut rows = conn.prepare(&format!("DESCRIBE {}", table_name))?;
        let schema = rows
            .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<(String, String)>>>()?;
        Ok(schema)
    }

    fn parse_table_names_from_query(&self, query: &str) -> Vec<String> {
        self.table_regex
            .find_iter(query)
            .map(|m| m.as_str().split(' ').next_back().unwrap().to_owned())
            .collect::<Vec<String>>()
    }

    // Helper function to convert a value from the row based on the type string
    fn get_value_by_type(row: &duckdb::Row, col_idx: usize, db_type: &str) -> Result<Value> {
        match db_type {
            "UBIGINT" => {
                let val: u64 = row.get(col_idx)?;
                // Convert u64 to Number (may need to use i64 for very large numbers)
                Ok(Value::Number(Number::from(val)))
            }
            "USMALLINT" => {
                let val: u16 = row.get(col_idx)?;
                Ok(Value::Number(Number::from(val)))
            }
            "VARCHAR(66)" | "VARCHAR" => {
                let val: String = row.get(col_idx)?;
                Ok(Value::String(val))
            }
            _ => {
                // Try as string for unknown types
                let val: String = row.get(col_idx)?;
                Ok(Value::String(val))
            }
        }
    }
}

impl StorageQuery for DuckDBStorage {
    fn list_events(&self) -> Result<Vec<EventDescriptorDb>> {
        // For StorageQuery methods, we clone to get a new connection (each request gets its own)
        let storage = self.clone();
        let conn = storage
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut rows = conn.prepare("SELECT * FROM event_descriptor")?;
        let events = rows
            .query_map([], |row| {
                Ok(EventDescriptorDb {
                    event_signature: row.get(0).unwrap(),
                    event_name: row.get(1).unwrap(),
                    event_hash: row.get(2).unwrap(),
                })
            })?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<EventDescriptorDb>>>()?;

        Ok(events)
    }

    fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> {
        // For StorageQuery methods, we clone to get a new connection (no Mutex needed)
        let storage = self.clone();
        let tables = storage
            .get_db_tables()?
            .iter()
            .filter(|table| table.starts_with("event_"))
            .map(|t| ContractDescriptorDb {
                contract_address: t.clone(),
                contract_name: None,
            })
            .collect::<Vec<ContractDescriptorDb>>();

        Ok(tables)
    }
    fn get_events(
        &self,
        event: Event,
        contract: Address,
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<EventDb>> {
        let storage = self.clone();

        let event_hash = event.selector().to_string();

        // Convert timestamps to Unix timestamps (seconds since epoch)
        let start_timestamp = start_time.timestamp() as u64;
        let end_timestamp = end_time
            .map(|dt| dt.timestamp() as u64)
            .unwrap_or(Utc::now().timestamp() as u64);

        // Normalize contract address to lowercase for case-insensitive comparison
        let contract_str = contract.to_string().to_lowercase();

        let conn = storage
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut stmt = conn.prepare(&format!(
            "
            SELECT e.block_number, e.transaction_hash, e.log_index, e.contract_address, 
                   e.topic0, e.topic1, e.topic2, e.topic3, b.block_timestamp
            FROM event_{event_hash} e
            NATURAL JOIN blocks b
            WHERE LOWER(e.contract_address) = LOWER(?) 
              AND b.block_timestamp >= ? 
              AND b.block_timestamp <= ?
            ORDER BY e.block_number
        "
        ))?;

        let events = stmt
            .query_map(
                params![
                    contract_str,
                    start_timestamp, // Pass as u64, not string
                    end_timestamp    // Pass as u64, not string
                ],
                |row| {
                    Ok(EventDb {
                        block_number: row.get(0)?,
                        transaction_hash: row.get(1)?,
                        log_index: row.get::<_, u16>(2)? as u64,
                        contract_address: row.get::<_, String>(3)?.parse().unwrap_or(contract),
                        topic0: row.get(4)?,
                        topic1: row.get::<_, Option<String>>(5)?,
                        topic2: row.get::<_, Option<String>>(6)?,
                        topic3: row.get::<_, Option<String>>(7)?,
                        block_timestamp: row.get::<_, u64>(8)?, // Read as u64 directly
                    })
                },
            )?
            .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
            .collect::<Result<Vec<EventDb>>>()?;

        Ok(events)
    }

    fn send_raw_query(&self, query: &str) -> Result<Value> {
        let storage = self.clone();
        if !query.contains("SELECT") {
            return Ok(json!({ "error": "Query must be a SELECT statement" }));
        }

        // First retrieve the table schema to figure out what we shall expect from the query
        let table_names = storage.parse_table_names_from_query(query);
        let table_schema = table_names
            .iter()
            .map(|t| storage.get_table_schema(t))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<(String, String)>>();

        let mut results = Vec::new();
        let conn = storage
            .conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to acquire lock: {}", e))?;
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let mut result_obj = Map::new();
            for (i, (col_name, col_type)) in table_schema.iter().enumerate() {
                let value = DuckDBStorage::get_value_by_type(row, i, col_type)?;
                result_obj.insert(col_name.clone(), value);
            }
            results.push(Value::Object(result_obj));
        }

        Ok(Value::Array(results))
    }
}

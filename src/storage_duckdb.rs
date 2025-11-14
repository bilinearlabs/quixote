// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the connection to the DuckDB database.

use crate::{ContractDescriptorDb, Event, EventDb, EventDescriptorDb, StorageQuery};
use alloy::{
    primitives::{Address, B256, keccak256},
    rpc::types::Log,
};
use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::{Connection, params};
use serde_json::{Map, Number, Value, json};
use std::string::ToString;
use std::sync::Mutex;

const DUCKDB_FILE_PATH: &str = "etherduck_indexer.duckdb";
const DUCKDB_SCHEMA_VERSION: &str = "0.1.0";
const DUCKDB_BASE_TABLE_NAME: &str = "etherduck_info";

pub trait Storage: Send + Sync + 'static {
    fn add_events(&self, events: &[Log]) -> Result<()>;
    fn last_block(&self) -> Result<u64>;
    fn first_block(&self) -> Result<u64>;
}

pub struct DuckDBStorage {
    conn: Mutex<Connection>,
    table_regex: regex::Regex,
}

impl Storage for DuckDBStorage {
    fn add_events(&self, events: &[Log]) -> Result<()> {
        if let Ok(mut conn) = self.conn.lock() {
            let tx = conn.transaction()?;

            // Populate the blocks table so the event_X table can reference the block_number from this table.
            {
                // Ensure unique blocks are added to the blocks table.
                let mut blocks_appender = tx.appender("blocks")?;
                let mut last_block_number = 0;
                for event in events {
                    if event.block_number.unwrap() != last_block_number {
                        blocks_appender.append_row(params![
                            event.block_number.unwrap().to_string(),
                            event.block_hash.unwrap().to_string(),
                            event.block_timestamp.unwrap().to_string()
                        ])?;
                        last_block_number = event.block_number.unwrap();
                    } else {
                        continue;
                    }
                }

                blocks_appender.flush()?;
            }

            let event_hash = events.first().unwrap().topic0().unwrap().to_string();

            {
                let mut event_appender = tx.appender(&format!("event_{}", event_hash))?;

                for log in events {
                    let topics = log.topics();
                    let topic0 = topics
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("Log missing topic0 (event signature)"))?;

                    // Extract log data
                    let block_number = log
                        .block_number
                        .ok_or_else(|| anyhow::anyhow!("Log missing block_number"))?
                        as u64;

                    let transaction_hash = log
                        .transaction_hash
                        .ok_or_else(|| anyhow::anyhow!("Log missing transaction_hash"))?
                        .to_string();

                    let log_index = log
                        .log_index
                        .ok_or_else(|| anyhow::anyhow!("Log missing log_index"))?
                        as u16;

                    let contract_address = log.address().to_string();

                    // Extract topics (up to 4 topics)
                    let topic0_str = topic0.to_string();
                    let topic1_str = topics.get(1).map(|t| t.to_string());
                    let topic2_str = topics.get(2).map(|t| t.to_string());
                    let topic3_str = topics.get(3).map(|t| t.to_string());

                    let topic1_val = topic1_str.as_deref().unwrap_or("");
                    let topic2_val = topic2_str.as_deref().unwrap_or("");
                    let topic3_val = topic3_str.as_deref().unwrap_or("");

                    event_appender.append_row(params![
                        block_number.to_string(),
                        transaction_hash,
                        log_index.to_string(),
                        contract_address,
                        topic0_str,
                        topic1_val,
                        topic2_val,
                        topic3_val,
                    ])?;
                }

                event_appender.flush()?;
            }

            // Explicitly commit the transaction
            tx.commit()?;
        }

        // If we reach this point, the entire transaction was executed, thus we can update the last block with the
        // value from the last of the list.
        let last_block_number = events.last().unwrap().block_number.unwrap();
        self.update_last_block(last_block_number)?;

        Ok(())
    }

    #[inline]
    fn last_block(&self) -> Result<u64> {
        if let Ok(conn) = self.conn.lock() {
            Ok(conn.query_row(
                &format!("SELECT last_block FROM {}", DUCKDB_BASE_TABLE_NAME),
                [],
                |row| row.get(0),
            )?)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }

    #[inline]
    fn first_block(&self) -> Result<u64> {
        if let Ok(conn) = self.conn.lock() {
            Ok(conn.query_row(
                &format!("SELECT first_block FROM {}", DUCKDB_BASE_TABLE_NAME),
                [],
                |row| row.get(0),
            )?)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }
}

impl DuckDBStorage {
    pub fn new() -> Result<DuckDBStorage> {
        println!("Using the default DB");
        Self::with_db(DUCKDB_FILE_PATH)
    }

    /// Creates a new DuckDBStorage with the given database path.
    pub fn with_db(db_path: &str) -> Result<DuckDBStorage> {
        let conn = Connection::open(db_path).expect("failed to open duckdb database");
        let conn_lock = Mutex::new(conn);

        let table_exists: bool = {
            let conn = conn_lock.lock().unwrap();
            conn.query_row(
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
            )?
        };

        if !table_exists {
            println!("Initializing DB...");
            DuckDBStorage::create_db_base(&conn_lock)?;
        } else {
            // Try to retrieve version from etherduck_info table using a query
            let version: String = {
                let conn = conn_lock.lock().unwrap();
                conn.query_row("SELECT version FROM etherduck_info LIMIT 1", [], |row| {
                    row.get(0)
                })?
            };

            if version != DUCKDB_SCHEMA_VERSION {
                println!("Your database is out of date. Please run the database upgrade.");
            }
        }

        println!("Your database is loaded and ready to use.");

        // This regex will match the table names after FROM and any JOINs (supports INNER, LEFT, RIGHT, FULL, CROSS).
        // It captures each table name in group 1, ignoring keywords.
        let table_regex =
            regex::Regex::new(r"(?i)(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();

        Ok(DuckDBStorage {
            conn: conn_lock,
            table_regex,
        })
    }

    pub fn include_events(&self, events: &[String]) -> Result<()> {
        for event in events {
            let hash = keccak256(event.as_bytes());
            let event_hash = B256::from(hash).to_string();
            DuckDBStorage::create_event_schema(&self.conn, &event_hash)?;
        }
        Ok(())
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
                block_hash VARCHAR(42) NOT NULL,
                block_timestamp UBIGINT NOT NULL,
                PRIMARY KEY (block_number)
            );
            CREATE TABLE IF NOT EXISTS event_descriptor(
                event_signature VARCHAR(42) NOT NULL,
                event_name VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                PRIMARY KEY (event_signature)
            );
            COMMIT;"
        );
        {
            let conn = conn.lock().unwrap();
            conn.execute_batch(&statement)?;
        }

        {
            let conn = conn.lock().unwrap();
            conn.execute(
                &format!(
                    "INSERT INTO {} (version, first_block, last_block)
                    VALUES (?, 0, 0);",
                    DUCKDB_BASE_TABLE_NAME
                ),
                [DUCKDB_SCHEMA_VERSION],
            )?;
        }

        {
            let conn = conn.lock().unwrap();
            conn.execute(
                "INSERT INTO event_descriptor (event_signature, event_name, event_type)
                VALUES (?, ?, ?);",
                [
                    B256::from(keccak256(b"Transfer")).to_string(),
                    "Transfer".to_owned(),
                    "ERC20".to_owned(),
                ],
            )?;
        }

        Ok(())
    }

    fn create_event_schema(conn: &Mutex<Connection>, event_hash: &str) -> Result<()> {
        let statement = &format!(
            "
                CREATE TABLE IF NOT EXISTS event_{event_hash}(
                    block_number UBIGINT NOT NULL,
                    transaction_hash VARCHAR(42) NOT NULL,
                    log_index USMALLINT NOT NULL,
                    contract_address VARCHAR(42) NOT NULL,
                    topic0 VARCHAR(42),
                    topic1 VARCHAR(42),
                    topic2 VARCHAR(42),
                    topic3 VARCHAR(42),
                    PRIMARY KEY (block_number, transaction_hash, log_index)
                );
            ",
        );

        let conn = conn.lock().unwrap();
        conn.execute(statement, [])?;

        Ok(())
    }

    #[inline]
    fn update_last_block(&self, block_number: u64) -> Result<()> {
        if let Ok(conn) = self.conn.lock() {
            conn.execute(
                &format!("UPDATE {DUCKDB_BASE_TABLE_NAME} SET last_block = ?"),
                [block_number.to_string()],
            )?;
        }
        Ok(())
    }

    #[inline]
    pub fn set_first_block(&self, block_number: u64) -> Result<()> {
        if let Ok(conn) = self.conn.lock() {
            conn.execute(
                &format!("UPDATE {DUCKDB_BASE_TABLE_NAME} SET first_block = ?"),
                [block_number.to_string()],
            )?;
        }
        Ok(())
    }

    fn get_db_tables(&self) -> Result<Vec<String>> {
        if let Ok(conn) = self.conn.lock() {
            let mut rows = conn.prepare("SHOW tables")?;
            let tables = rows
                .query_map([], |row| Ok(row.get(0).unwrap()))?
                .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
                .collect::<Result<Vec<String>>>()?;
            Ok(tables)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }

    // Returns pairs (Column Name, Column Type)
    fn get_table_schema(&self, table_name: &str) -> Result<Vec<(String, String)>> {
        if let Ok(conn) = self.conn.lock() {
            let mut rows = conn.prepare(&format!("DESCRIBE {}", table_name))?;
            let schema = rows
                .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))?
                .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
                .collect::<Result<Vec<(String, String)>>>()?;
            Ok(schema)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }

    fn parse_table_names_from_query(&self, query: &str) -> Vec<String> {
        self.table_regex
            .find_iter(query)
            .map(|m| m.as_str().split(' ').last().unwrap().to_owned())
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
            "VARCHAR(42)" | "VARCHAR" => {
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
        if let Ok(conn) = self.conn.lock() {
            let mut rows = conn.prepare("SELECT * FROM event_descriptor")?;
            let events = rows
                .query_map([], |row| {
                    Ok(EventDescriptorDb {
                        event_signature: row.get(0).unwrap(),
                        event_name: row.get(1).unwrap(),
                        event_type: row.get(2).unwrap(),
                    })
                })?
                .map(|r| r.map_err(|e| anyhow::anyhow!(e)))
                .collect::<Result<Vec<EventDescriptorDb>>>()?;

            Ok(events)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }

    fn list_contracts(&self) -> Result<Vec<ContractDescriptorDb>> {
        let tables = self
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
        _event: Event,
        contract: Address,
        start_time: DateTime<Utc>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<EventDb>> {
        if let Ok(conn) = self.conn.lock() {
            // TODO
            //let event_hash = keccak256(event.to_string().as_bytes()).to_string();
            let event_hash = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

            // Convert timestamps to Unix timestamps (seconds since epoch)
            let start_timestamp = start_time.timestamp() as u64;
            let end_timestamp = end_time
                .map(|dt| dt.timestamp() as u64)
                .unwrap_or(Utc::now().timestamp() as u64);

            // Normalize contract address to lowercase for case-insensitive comparison
            let contract_str = contract.to_string().to_lowercase();

            println!("Querying events for contract: {}", contract_str);
            println!("start_timestamp: {} ({})", start_timestamp, start_time);
            println!(
                "end_timestamp: {} ({})",
                end_timestamp,
                end_time
                    .map(|dt| dt.to_string())
                    .unwrap_or_else(|| "now".to_string())
            );

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

            println!("Found {} events", events.len());
            Ok(events)
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }

    fn send_raw_query(&self, query: &str) -> Result<Value> {
        if query.find("SELECT").is_none() {
            return Ok(json!({ "error": "Query must be a SELECT statement" }));
        }

        // First retrieve the table schema to figure out what we shall expect from the query
        let table_names = self.parse_table_names_from_query(query);
        let table_schema = table_names
            .iter()
            .map(|t| self.get_table_schema(t))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<(String, String)>>();

        if let Ok(conn) = self.conn.lock() {
            let mut results = Vec::new();
            let mut stmt = conn.prepare(query)?;
            let mut rows = stmt.query([])?;

            while let Some(row) = rows.next()? {
                let mut result_obj = Map::new();
                for (i, (col_name, col_type)) in table_schema.iter().enumerate() {
                    let value = DuckDBStorage::get_value_by_type(&row, i, col_type)?;
                    result_obj.insert(col_name.clone(), value);
                }
                results.push(Value::Object(result_obj));
            }

            Ok(Value::Array(results))
        } else {
            Err(anyhow::anyhow!("Failed to acquire lock on database"))
        }
    }
}

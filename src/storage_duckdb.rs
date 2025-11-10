// Copyright (C) 2025 Bilinear Labs - All Rights Reserved

//! Module that handles the connection to the DuckDB database.

use alloy::{
    primitives::{B256, keccak256},
    rpc::types::Log,
};
use anyhow::Result;
use duckdb::Connection;
use std::sync::Mutex;

const DUCKDB_FILE_PATH: &str = "etherduck_indexer.duckdb";
const DUCKDB_SCHEMA_VERSION: &str = "0.1.0";
const DUCKDB_BASE_TABLE_NAME: &str = "etherduck_info";

pub trait Storage: Send + Sync + 'static {
    fn add_events(&self, events: &[Log]) -> Result<()>;
}

pub struct DuckDBStorage {
    conn: Mutex<Connection>,
}

impl Storage for DuckDBStorage {
    fn add_events(&self, events: &[Log]) -> Result<()> {
        for log in events {
            // Extract event signature (topic0) - this is the event hash
            let topics = log.topics();
            let topic0 = topics
                .first()
                .ok_or_else(|| anyhow::anyhow!("Log missing topic0 (event signature)"))?;

            let event_hash = topic0.to_string();

            // Ensure the event table exists
            DuckDBStorage::create_event_schema(&self.conn, &event_hash)?;

            // Extract log data
            let block_number = log
                .block_number
                .ok_or_else(|| anyhow::anyhow!("Log missing block_number"))?
                as i64;

            let transaction_hash = log
                .transaction_hash
                .ok_or_else(|| anyhow::anyhow!("Log missing transaction_hash"))?
                .to_string();

            let log_index = log
                .log_index
                .ok_or_else(|| anyhow::anyhow!("Log missing log_index"))?
                as i16;

            let contract_address = log.address().to_string();

            // Extract topics (up to 4 topics)
            let topic0_str = topic0.to_string();
            let topic1_str = topics.get(1).map(|t| t.to_string());
            let topic2_str = topics.get(2).map(|t| t.to_string());
            let topic3_str = topics.get(3).map(|t| t.to_string());

            // Insert into the event table
            {
                let conn = self.conn.lock().unwrap();
                // Handle optional topics - use empty string for NULL
                let topic1_val = topic1_str.as_deref().unwrap_or("");
                let topic2_val = topic2_str.as_deref().unwrap_or("");
                let topic3_val = topic3_str.as_deref().unwrap_or("");

                // Use INSERT OR IGNORE to handle duplicates (DuckDB supports this)
                conn.execute(
                    &format!(
                        "INSERT OR IGNORE INTO event_{} (block_number, transaction_hash, log_index, contract_address, topic0, topic1, topic2, topic3) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                        event_hash
                    ),
                    [
                        &block_number.to_string(),
                        &transaction_hash,
                        &log_index.to_string(),
                        &contract_address,
                        &topic0_str,
                        topic1_val,
                        topic2_val,
                        topic3_val,
                    ],
                )?;
            }

            // Update blocks table if needed
            if let Some(block_hash) = log.block_hash {
                let block_timestamp = log.block_timestamp.unwrap_or(0) as i64;

                {
                    let conn = self.conn.lock().unwrap();
                    conn.execute(
                        "INSERT OR IGNORE INTO blocks (block_number, block_hash, block_timestamp) 
                        VALUES (?, ?, ?);",
                        [
                            &block_number.to_string(),
                            &block_hash.to_string(),
                            &block_timestamp.to_string(),
                        ],
                    )?;
                }
            }
        }

        Ok(())
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

        Ok(DuckDBStorage { conn: conn_lock })
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
                first_block BIGINT, 
                last_block BIGINT,
                PRIMARY KEY (version)
            );
            CREATE TABLE IF NOT EXISTS blocks(
                block_number BIGINT NOT NULL,
                block_hash VARCHAR(42) NOT NULL,
                block_timestamp BIGINT NOT NULL,
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
                    block_number BIGINT NOT NULL,
                    transaction_hash VARCHAR(42) NOT NULL,
                    log_index SMALLINT NOT NULL,
                    contract_address VARCHAR(42) NOT NULL,
                    topic0 VARCHAR(42),
                    topic1 VARCHAR(42),
                    topic2 VARCHAR(42),
                    topic3 VARCHAR(42),
                    PRIMARY KEY (block_number, transaction_hash)
                );
            ",
        );

        let conn = conn.lock().unwrap();
        conn.execute(statement, [])?;

        Ok(())
    }
}
